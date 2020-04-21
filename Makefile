.PHONY: test

install:
	go get -d -v ./... && go install -v ./...

test: vet
	go test -v ./...

test-cover: vet
	go test -v ./... -covermode=count -coverprofile=coverage.out

vet:
	go vet ./...

lint:
	golint ./...

ci-test: install test-cover coveralls

coveralls:
	go get golang.org/x/tools/cmd/cover
	go get github.com/mattn/goveralls
	git checkout ${TRAVIS_BRANCH}
	/go/bin/goveralls -coverprofile=coverage.out -service=travis-ci -repotoken ${COVERALLS_TOKEN}
