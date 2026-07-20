package response

import "time"

type Repository struct {
	ID          string    `json:"id"`
	Owner       string    `json:"owner"`
	Name        string    `json:"name"`
	FullName    string    `json:"full_name"`
	Description *string   `json:"description"`
	Stars       int       `json:"stars"`
	Language    *string   `json:"language"`
	HTMLURL     string    `json:"html_url"`
	Notes       string    `json:"notes"`
	FetchedAt   time.Time `json:"fetched_at"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}
