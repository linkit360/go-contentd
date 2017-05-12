.PHONY: config run build

VERSION=$(shell git describe --always --long --dirty)

version:
	 @echo Version IS $(VERSION)

config:
	rm ./server ; go build ;./server 

run:
	./server

rm:
	rm -f bin/contentd-linux-amd64; rm -f ~/linkit/contentd-linux-amd64;

build:
	export GOOS=linux; export GOARCH=amd64; \
	go build -ldflags "-s -w" -o bin/contentd-linux-amd64; \

cp:
	cp  bin/contentd-linux-amd64 ~/linkit; cp config/contentd.yml ~/linkit/

