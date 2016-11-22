.PHONY: dev run build

dev:
	rm ./server ; go build ;./server 

run:
	./server

rm:
	rm -v bin/contentd-linux-amd64; rm ~/linkit/contentd-linux-amd64;

build:
	export GOOS=linux; export GOARCH=amd64; \
        go build -ldflags "-s -w" -o bin/contentd-linux-amd64; \
	cp  bin/contentd-linux-amd64 ~/linkit; cp dev/contentd.yml ~/linkit/
