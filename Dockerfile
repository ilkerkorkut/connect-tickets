FROM golang:1.25.0-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
RUN go run entgo.io/ent/cmd/ent generate --feature sql/lock ./ent/schema && CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -ldflags='-s -w' -o /out/api ./cmd/api && CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -ldflags='-s -w' -o /out/worker ./cmd/worker

FROM gcr.io/distroless/static-debian12:nonroot AS api

COPY --from=build /out/api /api

USER nonroot:nonroot
ENTRYPOINT ["/api"]

FROM gcr.io/distroless/static-debian12:nonroot AS worker

COPY --from=build /out/worker /worker

USER nonroot:nonroot
ENTRYPOINT ["/worker"]
