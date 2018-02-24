FROM golang:1.9-alpine as builder
WORKDIR /build
RUN apk update && apk add git
RUN git clone https://github.com/aso930/st-agent.git
WORKDIR /build/st-agent
ENV GOPATH=/build
ENV GOBIN=$GOPATH/bin
RUN go get .
RUN go build .

FROM alpine:latest
COPY --from=builder /build/st-agent/st-agent /usr/bin/st-agent
ENTRYPOINT ["st-agent"]