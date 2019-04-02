FROM golang:1.11-alpine as builder
RUN apk update && apk add --no-cache git
# Set the Current Working Directory inside the container
WORKDIR $GOPATH/src/github.com/magpierre/ingest_csv
# Copy everything from the current directory to the PWD(Present Working Directory) inside the container
COPY . .

# Download all the dependencies
# https://stackoverflow.com/questions/28031603/what-do-three-dots-mean-in-go-command-line-invocations
RUN go get -d -v ./...
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /go/bin/ingest_csv .

FROM alpine:latest  
RUN apk --no-cache add ca-certificates
WORKDIR /root/
# Copy the Pre-built binary file from the previous stage
COPY --from=builder /go/bin/ingest_csv .
# Run the executable
ENTRYPOINT ["./ingest_csv"]
CMD [ "-h" ]