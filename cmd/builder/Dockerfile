# Copyright 2019 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FROM golang:1.12-stretch as build

WORKDIR /builder
COPY . .
RUN go build ./cmd/builder/builder.go
RUN cp prebuild /usr/local/bin

FROM debian:jessie-slim
COPY --from=build /usr/local/bin/builder /usr/local/bin
RUN apt-get update && apt-get install -y git-core
