#Builder stage 
FROM golang:1.18.4-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o main main.go


#RUN stage
FROM alpine
WORKDIR /app
COPY --from=builder /app/main .
CMD ["/app/main"]
ENV kafkaConnectionURL pkc-41p56.asia-south1.gcp.confluent.cloud:9092
ENV SASLMECHANISM PLAIN
ENV SASLUSER 2QWGO7JGYQJ5ZMGZ
ENV SASLPASSWORD JNt+0blpAMoWFSf5sbkgdAzvE2ty+8uVmBK22WJBqPJJtA2dMdR5bpcItZpAzRHd
ENV CLIENTID cryptoProducer
ENV KAFKA_TOPIC cryptoPrices
