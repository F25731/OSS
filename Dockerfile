FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -o /out/image-bed ./cmd/server

FROM alpine:3.20
RUN adduser -D -H -u 10001 app && mkdir -p /data/image-bed/images && chown -R app:app /data/image-bed
USER app
COPY --from=build /out/image-bed /usr/local/bin/image-bed
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/image-bed"]
