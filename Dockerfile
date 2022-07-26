FROM golang:1.18-buster

WORKDIR /app

COPY ./go.mod ./
COPY ./go.sum ./
RUN go mod download

COPY ./*.go ./

RUN go build -o td_exporter
ARG TD_API_KEY TD_API_HOST

EXPOSE "5000"

CMD ["./td_exporter"]
