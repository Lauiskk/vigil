# One Dockerfile, three services, selected by build argument. The services
# share a module and differ only in their entry point, so three near-identical
# files would be three places to forget to change something.
ARG GO_VERSION=1.26

FROM golang:${GO_VERSION}-alpine AS build
ARG SERVICE
WORKDIR /src

# Dependencies first, so a source-only change does not re-download the module
# graph on every build.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO off and a static build: the runtime stage has no libc to link against.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/service ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/service /service
USER nonroot:nonroot
ENTRYPOINT ["/service"]
