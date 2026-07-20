package repository

import "context"

type Store interface {
	Get(context.Context, string) (Repository, error)
	List(context.Context, ListInput) (Page, error)
	Statistics(context.Context) (Statistics, error)
}
