package main

import (
	"log"

	"entgo.io/ent/entc"
	"entgo.io/ent/entc/gen"
)

func main() {
	if err := entc.Generate("./ent/schema", &gen.Config{
		Target:  "./ent",
		Package: "github.com/ilkerkorkut/connect-tickets/ent",
		Features: []gen.Feature{
			gen.FeatureVersionedMigration,
		},
	}); err != nil {
		log.Fatalf("running ent codegen: %v", err)
	}
}
