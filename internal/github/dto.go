package github

import "time"

type Repository struct {
	Owner       string    `json:"owner"`
	Name        string    `json:"name"`
	FullName    string    `json:"full_name"`
	Description *string   `json:"description"`
	Stars       int       `json:"stars"`
	Language    *string   `json:"language"`
	HTMLURL     string    `json:"html_url"`
	FetchedAt   time.Time `json:"fetched_at"`
}

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
