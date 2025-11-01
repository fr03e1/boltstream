FROM golang:1.23 AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG CMD_PATH=./cmd/app
ARG OUTPUT_NAME=app

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -o /out/${OUTPUT_NAME} ${CMD_PATH}


FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app


ARG OUTPUT_NAME=app
COPY --from=build /out/${OUTPUT_NAME} /app/${OUTPUT_NAME}

EXPOSE 8080

RUN adduser -D -u 10001 appuser
USER appuser

ENTRYPOINT ["/app/app"]