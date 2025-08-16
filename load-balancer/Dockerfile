# Build
FROM golang:1.23-alpine AS builder
WORKDIR /src
COPY . .
RUN go mod init load-balancer && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" -o /lb

# Runtime
FROM gcr.io/distroless/static:nonroot
COPY --from=builder /lb /lb
ENV BACKENDS=""
EXPOSE 9999
USER nonroot:nonroot
ENTRYPOINT ["/lb"]
