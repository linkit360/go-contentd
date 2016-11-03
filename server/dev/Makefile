.PHONY: dev run senddev build sendprod

dev:
	rm ./server ; go build ;./server 

run:
	./server

build:
	rm -v bin/contentd-linux-amd64; \
	export GOOS=linux; export GOARCH=amd64; \
  go build -ldflags "-s -w" -o bin/contentd-linux-amd64

senddev:
	# ssh -i ~/.ssh/mobilink_centos_testing_stage.pem centos@35.154.8.158 rm /home/centos/linkit/contentd-linux-amd64
	scp -i ~/.ssh/mobilink_centos_testing_stage.pem bin/contentd-linux-amd64 centos@35.154.8.158:/home/centos/linkit/contentd-linux-amd64
	scp -i ~/.ssh/mobilink_centos_testing_stage.pem dev/contentd.yml centos@35.154.8.158:/home/centos/linkit/

config:
	scp -i ~/.ssh/mobilink_centos_testing_stage.pem dev/contentd.yml centos@35.154.8.158:/home/centos/linkit/

sendprod:
	ssh -i ~/.ssh/mobilink_centos.pem centos@52.66.23.201 rm /home/centos/linkit/contentd-linux-amd64
	scp -i ~/.ssh/mobilink_centos.pem bin/contentd-linux-amd64 centos@52.66.23.201:/home/centos/linkit/contentd-linux-amd64
	scp -i ~/.ssh/mobilink_centos.pem dev/contentd.prod.yml centos@52.66.23.201:/home/centos/linkit/

cqrcampaign:
	curl http://localhost:50302/cqr?t=campaign

cqrservice:
	curl http://localhost:50302/cqr?t=service

cqrcontent:
	curl http://localhost:50302/cqr?t=content

cqrservice_content:
	curl http://localhost:50302/cqr?t=service_content

cqrcontent_sent:
	curl http://localhost:50302/cqr?t=content_sent

call:
	curl curl -H 'Content-Type: application/json' -H 'Accept: application/json' --data-binary '{"jsonrpc":"1.0","id":"curltext","method":"SVC.GetContentByCampaign","params":{ }}' -H 'content-type:text/plain;' http://localhost:50302
