OS := $(shell uname)
VERSION := $(shell sh -c 'git describe --always --tags')
BRANCH := $(shell sh -c 'git rev-parse --abbrev-ref HEAD')
COMMIT := $(shell sh -c 'git rev-parse --short HEAD')
DOCKER        := $(shell which docker)
DOCKER_IMAGE  := docker-registry:5000/actiontech/universe-compiler-udup:v4


PROJECT_NAME  ?= dtle
VERSION       = 9.9.9.9

ifdef GOBIN
PATH := $(GOBIN):$(PATH)
else
PATH := $(subst :,/bin:,$(GOPATH))/bin:$(PATH)
endif

GOFLAGS := -mod=vendor

default: driver

driver:
	GO111MODULE=on go build $(GOFLAGS) -o dist/dtle -ldflags \
"-X github.com/actiontech/dtle/g.Version=$(VERSION) \
-X github.com/actiontech/dtle/g.GitCommit=$(COMMIT) \
-X github.com/actiontech/dtle/g.GitBranch=$(BRANCH)" \
		./cmd/nomad-plugin/main.go

build_with_coverage_report: build-coverage-report-tool coverage-report-pre-build build coverage-report-post-build

build-coverage-report-tool:
	GO111MODULE=on go install $(GOFLAGS) github.com/actiontech/dtle/vendor/github.com/ikarishinjieva/golang-live-coverage-report/cmd/golang-live-coverage-report

coverage-report-pre-build:
	PATH=${GOPATH}/bin:$$PATH golang-live-coverage-report \
	    -pre-build -raw-code-build-dir ./coverage-report-raw-code -raw-code-deploy-dir ./coverage-report-raw-code \
	    -bootstrap-outfile ./cmd/dtle/coverage_report_bootstrap.go -bootstrap-package-name main \
	    ./agent ./api ./utils ./cmd/dtle/command ./internal ./internal/g  ./internal/logger ./internal/models  ./internal/server ./internal/server/scheduler ./internal/server/store ./internal/client/driver ./internal/client/driver/kafka3 ./internal/client/driver/mysql ./internal/client/driver/mysql/base ./internal/client/driver/mysql/binlog ./internal/client/driver/mysql/sql ./internal/client/driver/mysql/util ./internal/client/driver/mysql/sqle/g ./internal/client/driver/mysql/sqle/inspector

coverage-report-post-build:
	PATH=${GOPATH}/bin:$$PATH golang-live-coverage-report \
	    -post-build -raw-code-build-dir ./coverage-report-raw-code -bootstrap-outfile ./cmd/dtle/coverage_report_bootstrap.go \
	    ./agent ./api ./utils ./cmd/dtle/command  ./internal ./internal/g  ./internal/logger ./internal/models  ./internal/server ./internal/server/scheduler ./internal/server/store ./internal/client/driver ./internal/client/driver/kafka3 ./internal/client/driver/mysql ./internal/client/driver/mysql/base ./internal/client/driver/mysql/binlog ./internal/client/driver/mysql/sql ./internal/client/driver/mysql/util ./internal/client/driver/mysql/sqle/g ./internal/client/driver/mysql/sqle/inspector

package-common: driver
	rm -rf dist/install
	mkdir -p dist/install/usr/share/dtle/nomad-plugin
	cp -R dist/dtle dist/install/usr/share/dtle/nomad-plugin
	cp -R scripts dist/install/usr/share/dtle/
	cp -R etc dist/install/
	-mkdir -p dist/install/usr/share/dtle/ui
	-cp -R  ui dist/install/usr/share/dtle

package: package-common
	mkdir -p dist/install/usr/bin
	curl -o dist/nomad.zip "ftp://${RELEASE_FTPD_HOST}/binary/nomad_1.1.2_linux_amd64.zip"
	curl -o dist/consul.zip "ftp://${RELEASE_FTPD_HOST}/binary/consul_1.7.2_linux_amd64.zip"
	mkdir -p dist/install/usr/bin
	cd dist/install/usr/bin && unzip ../../../nomad.zip && unzip ../../../consul.zip
	cd dist && fpm --force -s dir -t rpm -n $(PROJECT_NAME) -v $(VERSION) -C install \
      --before-install ../misc/pre-install.sh \
      --after-install ../misc/post-install.sh \
      --before-remove ../misc/pre-remove.sh \
      --after-remove ../misc/post-remove.sh \
      --before-upgrade ../misc/pre-upgrade.sh \
      --after-upgrade ../misc/post-upgrade.sh \
      --config-files etc \
      --depends iproute
	cd dist && md5sum $(PROJECT_NAME)-$(VERSION)-1.x86_64.rpm > $(PROJECT_NAME)-$(VERSION).x86_64.rpm.md5

vet:
	go vet ./...

fmt:
	gofmt -s -w .

docker_test:
	$(DOCKER) run -v $(shell pwd)/:/universe/src/github.com/actiontech/dtle --rm $(DOCKER_IMAGE) -c "cd /universe/src/github.com/actiontech/dtle/drivers && go test -cover -v -mod=vendor ./..."

test:
	cd drivers && go test -cover -v -mod=vendor ./...

mtswatcher: helper/mtswatcher/mtswatcher.go
	GO111MODULE=on go build $(GOFLAGS) -o dist/mtswatcher ./helper/mtswatcher/mtswatcher.go

docker_rpm:
	$(DOCKER) run -v $(shell pwd)/:/universe/src/github.com/actiontech/dtle --rm $(DOCKER_IMAGE) -c "cd /universe/src/github.com/actiontech/dtle; GOPATH=/universe make RELEASE_FTPD_HOST=${RELEASE_FTPD_HOST} package ;chmod -R ugo+rw dist;"

docker_rpm_with_coverage_report:
	#$(DOCKER) run -v $(shell pwd)/:/universe/src/github.com/actiontech/dtle --rm $(DOCKER_IMAGE) -c "cd /universe/src/github.com/actiontech/dtle; GOPATH=/universe make prepare build-coverage-report-tool coverage-report-pre-build package coverage-report-post-build ;chmod -R ugo+rw dist;"
	echo TODO

generate_swagger_docs:
	swag init -g ./drivers/api/route.go -o ./drivers/api/docs

upload:
	curl --ftp-create-dirs -T $(shell pwd)/dist/*.rpm ftp://${RELEASE_FTPD_HOST}/actiontech-${PROJECT_NAME}/qa/${VERSION}/${PROJECT_NAME}-${VERSION}-qa.x86_64.rpm
	curl --ftp-create-dirs -T $(shell pwd)/dist/*.rpm.md5 ftp://${RELEASE_FTPD_HOST}/actiontech-${PROJECT_NAME}/qa/${VERSION}/${PROJECT_NAME}-${VERSION}-qa.x86_64.rpm.md5

upload_with_coverage_report:
	#curl --ftp-create-dirs -T $(shell pwd)/dist/*.rpm ftp://${RELEASE_FTPD_HOST}/actiontech-${PROJECT_NAME}/qa/${VERSION}/${PROJECT_NAME}-${VERSION}-qa.coverage.x86_64.rpm
	#curl --ftp-create-dirs -T $(shell pwd)/dist/*.rpm.md5 ftp://${RELEASE_FTPD_HOST}/actiontech-${PROJECT_NAME}/qa/${VERSION}/${PROJECT_NAME}-${VERSION}-qa.coverage.x86_64.rpm.md5
	echo TODO

.PHONY: vet fmt build default driver
