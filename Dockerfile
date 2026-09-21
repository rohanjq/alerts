FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /alertsd ./cmd/alertsd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /alertsd /alertsd
USER nonroot:nonroot
ENTRYPOINT ["/alertsd"]
