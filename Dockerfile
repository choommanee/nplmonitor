FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o sgf-api .

FROM alpine:latest
RUN apk add --no-cache ca-certificates tzdata
ENV TZ=Asia/Bangkok
WORKDIR /root/
COPY --from=builder /app/sgf-api .
COPY --from=builder /app/index.html .
EXPOSE 3000
CMD ["./sgf-api"]
