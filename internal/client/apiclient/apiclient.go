package apiclient

import (
	"github.com/ilkerkorkut/connect-tickets/config"
	"github.com/ilkerkorkut/connect-tickets/internal/client"
	"github.com/ilkerkorkut/connect-tickets/internal/client/github"
)

type apiClient struct {
	github client.GitHub
}

func NewClient(cfg config.Config) (client.APIClient, error) {
	githubClient, err := github.NewClient(cfg.GitHub, nil)
	if err != nil {
		return nil, err
	}
	return &apiClient{github: githubClient}, nil
}

func (c *apiClient) GitHub() client.GitHub {
	return c.github
}
