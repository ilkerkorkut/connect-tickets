FROM golang:1.25.0-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .
RUN go generate ./... && go run entgo.io/ent/cmd/ent generate ./ent/schema && git diff --exit-code -- ent migrations && CGO_ENABLED=0 GOOS=linux go build -trimpath -buildvcs=false -ldflags='-s -w' -o /out/github-tracker ./cmd/github-tracker

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/github-tracker /github-tracker

USER nonroot:nonroot
ENTRYPOINT ["/github-tracker"]
CMD ["all"]
