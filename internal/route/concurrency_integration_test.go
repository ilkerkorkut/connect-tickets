//go:build testcontainers

package route

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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
	"github.com/ilkerkorkut/connect-tickets/internal/testkit"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
)

const concurrentRequests = 5

func TestConcurrentRepositoryCreation(t *testing.T) {
	log := newScenarioLogger(t)
	log.Logf("scenario=concurrent_repository_creation clients=%d expected_github_requests=1 expected_created=1 expected_conflicts=%d", concurrentRequests, concurrentRequests-1)
	log.Log("scenario_detail=all_clients_post_the_same_repository_identity; shared_start_gate_releases_them_together; distributed_cache_lock_coalesces_the_cache_miss; database_uniqueness_allows_one_creation")
	log.Log("phase=setup action=start_isolated_dependencies")
	environment := testkit.Start(t)
	log.Log("phase=setup action=dependencies_ready components=postgres,redis,kafka")
	client := newEntClient(t, environment.DatabaseURL)
	cache := newRedisClient(t, environment.RedisAddress)
	owner := "smoke-" + uuid.NewString()
	name := "concurrency"
	log.Logf("phase=setup action=create_repository_identity owner=%s name=%s", owner, name)
	t.Cleanup(func() {
		log.Log("phase=cleanup action=delete_repository_rows_and_cache_keys")
		cleanupRepository(client, cache, owner, name)
	})
	github := newGitHubMock(t, log, owner, name)
	api := newAPI(t, log, client, cache, github.server.URL)
	responses := sendConcurrentCreates(t, log, api.URL, owner, name)
	assertConcurrentCreate(t, log, responses, github.calls.Load())
}

func newEntClient(t *testing.T, databaseURL string) *ent.Client {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	client := ent.NewClient(ent.Driver(entsql.OpenDB(dialect.Postgres, stdlib.OpenDBFromPool(pool))))
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func newRedisClient(t *testing.T, address string) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: address})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	return client
}

func newAPI(t *testing.T, log *scenarioLogger, client *ent.Client, cache *redis.Client, githubURL string) *httptest.Server {
	t.Helper()
	upstream, err := githubclient.NewClient(config.GitHubConfig{BaseURL: githubURL, Timeout: 2 * time.Second, UserAgent: "concurrency-smoke", MaxIdleConns: concurrentRequests, MaxIdleConnsPerHost: concurrentRequests, IdleConnTimeout: time.Minute}, nil)
	if err != nil {
		t.Fatal(err)
	}
	gateway, err := githubcache.NewGateway(upstream, cache, config.CacheConfig{TTL: time.Minute, LockTTL: 3 * time.Second, LockWait: 4 * time.Second}, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(NewRouter(Dependencies{Repositories: repository.NewService(client, gateway)}, config.HTTPConfig{RequestTimeout: 5 * time.Second}, nil))
	t.Cleanup(server.Close)
	log.Log("phase=setup action=api_ready request_timeout=5s github_timeout=2s cache_ttl=1m cache_lock_ttl=3s cache_lock_wait=4s")
	return server
}

func cleanupRepository(client *ent.Client, cache *redis.Client, owner, name string) {
	ctx := context.Background()
	normalizedFullName := strings.ToLower(owner + "/" + name)
	rows, err := client.Repository.Query().Where(entrepository.NormalizedFullNameEQ(normalizedFullName)).All(ctx)
	if err == nil {
		for _, row := range rows {
			_, _ = client.RepositoryChange.Delete().Where(repositorychange.RepoIDEQ(row.ID)).Exec(ctx)
			_ = client.Repository.DeleteOneID(row.ID).Exec(ctx)
		}
	}
	_ = cache.Del(ctx, "github:repo:v1:"+normalizedFullName, "github:repo-lock:v1:"+normalizedFullName).Err()
}

type githubMock struct {
	server *httptest.Server
	calls  atomic.Int32
}

func newGitHubMock(t *testing.T, log *scenarioLogger, owner, name string) *githubMock {
	t.Helper()
	mock := &githubMock{}
	mock.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/repos/"+owner+"/"+name {
			http.NotFound(writer, request)
			return
		}
		call := mock.calls.Add(1)
		log.Logf("phase=github_mock call=%d method=%s path=%s outcome=repository_payload", call, request.Method, request.URL.Path)
		writer.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(writer, `{"name":%q,"full_name":%q,"stargazers_count":1,"html_url":%q,"owner":{"login":%q}}`, name, owner+"/"+name, "https://github.example/"+owner+"/"+name, owner)
	}))
	t.Cleanup(mock.server.Close)
	log.Logf("phase=setup action=github_mock_ready expected_path=/repos/%s/%s", owner, name)
	return mock
}

func sendConcurrentCreates(t *testing.T, log *scenarioLogger, apiURL, owner, name string) []smokeResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	start := make(chan struct{})
	responses := make(chan smokeResponse, concurrentRequests)
	errors := make(chan error, concurrentRequests)
	var requests sync.WaitGroup
	log.Logf("phase=execution action=launch_clients count=%d synchronization=shared_start_gate", concurrentRequests)
	for requestNumber := range concurrentRequests {
		requests.Add(1)
		go func(number int) {
			defer requests.Done()
			<-start
			log.Logf("phase=execution request=%d action=post_repository", number)
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL+"/api/repos", strings.NewReader(`{"owner":"`+owner+`","name":"`+name+`"}`))
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
			log.Logf("phase=execution request=%d action=response status=%d", number, response.StatusCode)
			responses <- smokeResponse{number: number, status: response.StatusCode, body: string(body)}
		}(requestNumber + 1)
	}
	log.Log("phase=execution action=release_shared_start_gate")
	close(start)
	requests.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	close(responses)
	result := make([]smokeResponse, 0, concurrentRequests)
	for response := range responses {
		result = append(result, response)
	}
	return result
}

func assertConcurrentCreate(t *testing.T, log *scenarioLogger, responses []smokeResponse, githubCalls int32) {
	t.Helper()
	created := 0
	conflicts := 0
	var unexpected []smokeResponse
	for _, response := range responses {
		switch response.status {
		case http.StatusCreated:
			created++
		case http.StatusConflict:
			conflicts++
		default:
			unexpected = append(unexpected, response)
		}
	}
	log.Logf("phase=assertion github_requests=%d created=%d conflicts=%d unexpected=%d", githubCalls, created, conflicts, len(unexpected))
	if len(unexpected) > 0 {
		t.Fatalf("unexpected responses = %#v", unexpected)
	}
	if created != 1 {
		t.Fatalf("created responses = %d, want 1", created)
	}
	if conflicts != concurrentRequests-1 {
		t.Fatalf("conflict responses = %d, want %d", conflicts, concurrentRequests-1)
	}
	if githubCalls != 1 {
		t.Fatalf("GitHub requests = %d, want 1", githubCalls)
	}
}

type smokeResponse struct {
	number int
	status int
	body   string
}

type scenarioLogger struct {
	t    *testing.T
	mu   sync.Mutex
	step int
}

func newScenarioLogger(t *testing.T) *scenarioLogger {
	t.Helper()
	return &scenarioLogger{t: t}
}

func (logger *scenarioLogger) Log(format string, args ...any) {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	logger.step++
	logger.t.Logf("step=%02d "+format, append([]any{logger.step}, args...)...)
}

func (logger *scenarioLogger) Logf(format string, args ...any) {
	logger.Log(format, args...)
}
