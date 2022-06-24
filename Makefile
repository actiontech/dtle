OS := $(shell uname)
VERSION := $(shell sh -c 'git describe --always --tags')
BRANCH := $(shell sh -c 'git rev-parse --abbrev-ref HEAD')
COMMIT := $(shell sh -c 'git rev-parse --short HEAD')
DOCKER        := $(shell which docker)
DOCKER_IMAGE  := docker-registry:5000/actiontech/universe-compiler-udup:v6


PROJECT_NAME  ?= dtle
VERSION       = 4.22.06.0

ifdef GOBIN
PATH := $(GOBIN):$(PATH)
else
PATH := $(subst :,/bin:,$(GOPATH))/bin:$(PATH)
endif

GOFLAGS := -mod=vendor

default: driver

driver:
	go build $(GOFLAGS) -o dist/dtle -ldflags \
"-X github.com/actiontech/dtle/g.Version=$(VERSION) \
-X github.com/actiontech/dtle/g.GitCommit=$(COMMIT) \
-X github.com/actiontech/dtle/g.GitBranch=$(BRANCH)" \
		./cmd/nomad-plugin/main.go

build_with_coverage_report: build-coverage-report-tool coverage-report-pre-build package coverage-report-post-build

build-coverage-report-tool:
	go install $(GOFLAGS) github.com/actiontech/dtle/vendor/github.com/actiontech/golang-live-coverage-report/cmd/golang-live-coverage-report

coverage-report-pre-build:
	PATH=${GOPATH}/bin:$$PATH golang-live-coverage-report \
	    -pre-build -raw-code-build-dir ./coverage-report-raw-code -raw-code-deploy-dir ./coverage-report-raw-code \
	    -bootstrap-outfile ./api/coverage_report_bootstrap.go -bootstrap-package-name api \
	   ./api ./api/handler ./api/handler/v1 ./api/handler/v2 ./api/models ./driver ./driver/common ./driver/kafka ./driver/mysql ./driver/mysql/base ./driver/mysql/binlog ./driver/mysql/mysqlconfig ./driver/mysql/sql ./driver/mysql/sqle/g ./driver/mysql/sqle/inspector ./driver/mysql/util ./g

coverage-report-post-build:
	PATH=${GOPATH}/bin:$$PATH golang-live-coverage-report \
	    -post-build -raw-code-build-dir ./coverage-report-raw-code -bootstrap-outfile ./api/coverage_report_bootstrap.go \
	    ./api ./api/handler ./api/handler/v1 ./api/handler/v2 ./api/models ./driver ./driver/common ./driver/kafka ./driver/mysql ./driver/mysql/base ./driver/mysql/binlog ./driver/mysql/mysqlconfig ./driver/mysql/sql ./driver/mysql/sqle/g ./driver/mysql/sqle/inspector ./driver/mysql/util ./g

package-common: driver
	rm -rf dist/install
	mkdir -p dist/install/usr/share/dtle/nomad-plugin
	cp -R dist/dtle dist/install/usr/share/dtle/nomad-plugin
	cp -R scripts dist/install/usr/share/dtle/
	cp -R etc dist/install/
	-mkdir -p dist/install/usr/share/dtle/ui
	-cp -R  ui dist/install/usr/share/dtle
	-cp -R ./coverage-report-raw-code dist/install/usr/share/dtle/nomad-plugin/coverage-report-raw-code

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
	cd dist && mv $(PROJECT_NAME)-$(VERSION)-1.x86_64.rpm $(PROJECT_NAME)-$(VERSION).x86_64.rpm && \
	md5sum $(PROJECT_NAME)-$(VERSION).x86_64.rpm > $(PROJECT_NAME)-$(VERSION).x86_64.rpm.md5

vet:
	go vet ./...

fmt:
	gofmt -s -w .

docker_test:
	$(DOCKER) run -v $(shell pwd)/:/universe/src/github.com/actiontech/dtle --rm $(DOCKER_IMAGE) -c "cd /universe/src/github.com/actiontech/dtle/driver && go test -cover -v -mod=vendor ./..."

test:
	cd driver && go test -cover -v -mod=vendor ./...

mtswatcher: helper/mtswatcher/mtswatcher.go
	go build $(GOFLAGS) -o dist/mtswatcher ./helper/mtswatcher/mtswatcher.go

docker_rpm:
	$(DOCKER) run -v $(shell pwd)/:/universe/src/github.com/actiontech/dtle --rm -e PROJECT_NAME=$(PROJECT_NAME) $(DOCKER_IMAGE) -c "cd /universe/src/github.com/actiontech/dtle; GOPATH=/universe make RELEASE_FTPD_HOST=${RELEASE_FTPD_HOST} package ;chmod -R ugo+rw dist;"

docker_rpm_with_coverage_report:
	$(DOCKER) run -v $(shell pwd)/:/universe/src/github.com/actiontech/dtle --rm -e PROJECT_NAME=$(PROJECT_NAME) $(DOCKER_IMAGE) -c "cd /universe/src/github.com/actiontech/dtle; GOPATH=/universe make RELEASE_FTPD_HOST=${RELEASE_FTPD_HOST} build_with_coverage_report ;chmod -R ugo+rw dist;"

generate_swagger_docs:
	swag init -g ./api/route.go -o ./api/docs

upload:
	curl --ftp-create-dirs -T $(shell pwd)/dist/*.rpm ftp://${RELEASE_FTPD_HOST}/actiontech-${PROJECT_NAME}/qa/${VERSION}/${PROJECT_NAME}-${VERSION}.x86_64.rpm
	curl --ftp-create-dirs -T $(shell pwd)/dist/*.rpm.md5 ftp://${RELEASE_FTPD_HOST}/actiontech-${PROJECT_NAME}/qa/${VERSION}/${PROJECT_NAME}-${VERSION}.x86_64.rpm.md5

upload_with_coverage_report:
	curl --ftp-create-dirs -T $(shell pwd)/dist/*.rpm ftp://${RELEASE_FTPD_HOST}/actiontech-${PROJECT_NAME}/qa/${VERSION}/${PROJECT_NAME}-${VERSION}.coverage.x86_64.rpm
	curl --ftp-create-dirs -T $(shell pwd)/dist/*.rpm.md5 ftp://${RELEASE_FTPD_HOST}/actiontech-${PROJECT_NAME}/qa/${VERSION}/${PROJECT_NAME}-${VERSION}.coverage.x86_64.rpm.md5


.PHONY: vet fmt build default driver
