FROM golang:1.12-stretch as build

WORKDIR /builder
COPY . .
RUN go build ./cmd/builder/builder.go
RUN cp prebuild /usr/local/bin

FROM debian:jessie-slim
COPY --from=build /usr/local/bin/builder /usr/local/bin
RUN apt-get update && apt-get install -y git-core
