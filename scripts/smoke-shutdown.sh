#!/usr/bin/env bash
set -euo pipefail

project="${COMPOSE_PROJECT:-github-tracker}"
docker compose --project-name "$project" -f compose.yaml stop --timeout 35 api worker publisher
docker compose --project-name "$project" -f compose.yaml start api worker publisher
