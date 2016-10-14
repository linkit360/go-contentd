.PHONY: dev run release

dev:
	go build -v -o bin/contentd \
	./bin/contentd

run:
	./bin/contentd

release:
	export GOOS=linux; export GOARCH=amd64; \
  go build -ldflags "-s -w" -o bin/contentd-linux-amd64
