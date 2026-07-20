package github

import "github.com/ilkerkorkut/connect-tickets/internal/client"

type Repository = client.Repository

type repositoryResponse struct {
	Name        *string `json:"name"`
	FullName    *string `json:"full_name"`
	Description *string `json:"description"`
	Stargazers  *int    `json:"stargazers_count"`
	Language    *string `json:"language"`
	HTMLURL     *string `json:"html_url"`
	Owner       owner   `json:"owner"`
}

type owner struct {
	Login *string `json:"login"`
}
