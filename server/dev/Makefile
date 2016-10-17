.PHONY: dev run release send

dev:
	go build -v -o bin/contentd \
	./bin/contentd

run:
	./bin/contentd

release:
	export GOOS=linux; export GOARCH=amd64; \
  go build -ldflags "-s -w" -o bin/contentd-linux-amd64

send:
	scp -i ~/.ssh/mobilink_centos.pem bin/contentd-linux-amd64 centos@52.66.23.201:/home/centos/go/src/github.com/vostrok/contentd/server/bin/contentd-linux-amd64
