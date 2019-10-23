FROM golang:1.12-stretch as build

WORKDIR /builder
COPY . .
RUN go build ./cmd/builder
RUN cp prebuild /usr/local/bin

FROM debian:jessie-slim
COPY --from=build /usr/local/bin/prebuild /usr/local/bin
