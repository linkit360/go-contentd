.PHONY: dev run build

VERSION=$(shell git describe --always --long --dirty)

version:
	 @echo Version IS $(VERSION)

dev:
	rm ./server ; go build ;./server 

run:
	./server

rm:
	rm -v bin/contentd-linux-amd64; rm ~/linkit/contentd-linux-amd64;

build:
	export GOOS=linux; export GOARCH=amd64; \
	sed -i "s/%VERSION%/$(VERSION)/g" /home/centos/vostrok/utils/metrics/metrics.go; \
	go build -ldflags "-s -w" -o bin/contentd-linux-amd64; \


cp:
	cp  bin/contentd-linux-amd64 ~/linkit; cp dev/contentd.yml ~/linkit/

