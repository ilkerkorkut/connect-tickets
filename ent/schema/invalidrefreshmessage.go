package schema

import (
	"time"

	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

type InvalidRefreshMessage struct {
	ent.Schema
}

func (InvalidRefreshMessage) Fields() []ent.Field {
	return []ent.Field{
		field.String("id").StorageKey("event_key").NotEmpty().MaxLen(128).Immutable(),
		field.String("error_code").NotEmpty().MaxLen(64),
		field.Bytes("payload"),
		field.Time("recorded_at").Default(time.Now),
	}
}

func (InvalidRefreshMessage) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("recorded_at"),
	}
}
