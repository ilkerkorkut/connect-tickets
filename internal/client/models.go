package client

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
