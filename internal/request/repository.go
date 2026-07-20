package request

type CreateRepository struct {
	Owner string `json:"owner"`
	Name  string `json:"name"`
}

type UpdateRepositoryNotes struct {
	Notes string `json:"notes"`
}
