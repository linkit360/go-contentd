FROM golang:1.8.0-alpine
EXPOSE 50301 50302

ENV TERM xterm
RUN apk add --progress --no-cache \
    bash wget curl vim \
    icu-libs icu-dev \
    git zip unzip zlib-dev \
    sudo su-exec openssh-client

RUN addgroup -S -g 1000 docker
RUN adduser -S -D -s /bin/bash -u 1000 -G docker docker

# SSH && CONFIG
COPY config/ /config
RUN mv /config/ssh /home/docker/.ssh
RUN cp /config/.bashrc /home/docker
RUN mv /config/.bashrc /root

RUN chown -R 1000:1000 /home/docker
RUN chmod -R 0700 /home/docker

COPY src/ /go/src

RUN chown -R 1000:1000 /go
RUN chmod -R 0700 /go

WORKDIR /go/src
USER 1000
RUN git config --global url."git@github.com:".insteadOf "https://github.com/"
RUN go get ./...

#ENTRYPOINT server
ENTRYPOINT ["bash", "/config/idle.sh"]
