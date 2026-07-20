//go:build integration

package route

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/google/uuid"
	"github.com/ilkerkorkut/connect-tickets/config"
	"github.com/ilkerkorkut/connect-tickets/ent"
	entrepository "github.com/ilkerkorkut/connect-tickets/ent/repository"
	"github.com/ilkerkorkut/connect-tickets/ent/repositorychange"
	githubcache "github.com/ilkerkorkut/connect-tickets/internal/cache/github"
	githubclient "github.com/ilkerkorkut/connect-tickets/internal/client/github"
	"github.com/ilkerkorkut/connect-tickets/internal/service/repository"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
)

func TestConcurrentRepositoryCreation(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	redisAddress := os.Getenv("TEST_REDIS_ADDR")
	if databaseURL == "" || redisAddress == "" {
		t.Skip("TEST_DATABASE_URL and TEST_REDIS_ADDR are required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	entClient := ent.NewClient(ent.Driver(entsql.OpenDB(dialect.Postgres, stdlib.OpenDBFromPool(pool))))
	t.Cleanup(func() { _ = entClient.Close() })
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddress})
	t.Cleanup(func() { _ = redisClient.Close() })
	if err := redisClient.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	owner := "smoke-" + uuid.NewString()
	name := "concurrency"
	normalizedFullName := strings.ToLower(owner + "/" + name)
	t.Cleanup(func() {
		rows, err := entClient.Repository.Query().Where(entrepository.NormalizedFullNameEQ(normalizedFullName)).All(context.Background())
		if err != nil {
			return
		}
		for _, row := range rows {
			_, _ = entClient.RepositoryChange.Delete().Where(repositorychange.RepoIDEQ(row.ID)).Exec(context.Background())
			_ = entClient.Repository.DeleteOneID(row.ID).Exec(context.Background())
		}
		_ = redisClient.Del(context.Background(), "github:repo:v1:"+normalizedFullName, "github:repo-lock:v1:"+normalizedFullName).Err()
	})

	upstreamStarted := make(chan struct{}, 1)
	releaseUpstream := make(chan struct{})
	var upstreamCalls atomic.Int32
	githubServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/repos/"+owner+"/"+name {
			http.NotFound(writer, request)
			return
		}
		upstreamCalls.Add(1)
		upstreamStarted <- struct{}{}
		<-releaseUpstream
		writer.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(writer, `{"name":%q,"full_name":%q,"stargazers_count":1,"html_url":%q,"owner":{"login":%q}}`, name, owner+"/"+name, "https://github.example/"+owner+"/"+name, owner)
	}))
	t.Cleanup(githubServer.Close)

	upstream, err := githubclient.NewClient(config.GitHubConfig{BaseURL: githubServer.URL, Timeout: 2 * time.Second, UserAgent: "concurrency-smoke", MaxIdleConns: 4, MaxIdleConnsPerHost: 4, IdleConnTimeout: time.Minute}, nil)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := githubcache.NewGateway(upstream, redisClient, config.CacheConfig{TTL: time.Minute, LockTTL: 3 * time.Second, LockWait: 4 * time.Second}, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	service := repository.NewService(entClient, gateway)
	requestsEntered := make(chan struct{}, 5)
	router := NewRouter(Dependencies{Repositories: service}, config.HTTPConfig{RequestTimeout: 5 * time.Second}, nil)
	apiServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestsEntered <- struct{}{}
		router.ServeHTTP(writer, request)
	}))
	t.Cleanup(apiServer.Close)

	start := make(chan struct{})
	responses := make(chan smokeResponse, 5)
	errors := make(chan error, 5)
	var requests sync.WaitGroup
	for range 5 {
		requests.Add(1)
		go func() {
			defer requests.Done()
			<-start
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, apiServer.URL+"/api/repos", strings.NewReader(`{"owner":"`+owner+`","name":"`+name+`"}`))
			if err != nil {
				errors <- err
				return
			}
			request.Header.Set("Content-Type", "application/json")
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				errors <- err
				return
			}
			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil {
				errors <- err
				return
			}
			responses <- smokeResponse{status: response.StatusCode, body: string(body)}
		}()
	}
	close(start)
	for range 5 {
		<-requestsEntered
	}
	<-upstreamStarted
	close(releaseUpstream)
	requests.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	close(responses)
	created := 0
	conflicts := 0
	var unexpected []smokeResponse
	for response := range responses {
		switch response.status {
		case http.StatusCreated:
			created++
		case http.StatusConflict:
			conflicts++
		default:
			unexpected = append(unexpected, response)
		}
	}
	if len(unexpected) > 0 {
		t.Fatalf("unexpected responses = %#v", unexpected)
	}
	if created != 1 {
		t.Fatalf("created responses = %d, want 1", created)
	}
	if conflicts != 4 {
		t.Fatalf("conflict responses = %d, want 4", conflicts)
	}
	if upstreamCalls.Load() != 1 {
		t.Fatalf("GitHub requests = %d, want 1", upstreamCalls.Load())
	}
}

type smokeResponse struct {
	status int
	body   string
}
