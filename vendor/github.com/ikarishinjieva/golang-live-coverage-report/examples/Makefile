default: pre_build build post_build

pre_build:
	golang-live-coverage-report -pre-build -raw-code-build-dir ./raw -raw-code-deploy-dir ./deploy -bootstrap-package-name main -bootstrap-outfile ./cover_bootstrap.go ./coveree.go

build:
	go build -o demo *.go
	cp -R ./raw ./deploy

post_build:
	golang-live-coverage-report -post-build -raw-code-build-dir ./raw/ -bootstrap-outfile ./cover_bootstrap.go ./coveree.go

clean:
	rm -rf ./deploy
