module github.com/actiontech/dtle

go 1.16

require (
	github.com/Shopify/sarama v1.26.4
	github.com/actiontech/golang-live-coverage-report v0.0.0-20210902074032-43aa91afdc2c
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751
	github.com/araddon/qlbridge v0.0.0-00010101000000-000000000000
	github.com/armon/go-metrics v0.3.4
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/docker/libkv v0.2.1
	github.com/go-mysql-org/go-mysql v1.1.3-0.20210705101833-83965e516929
	github.com/go-playground/universal-translator v0.17.0 // indirect
	github.com/go-playground/validator v9.31.0+incompatible
	github.com/go-sql-driver/mysql v1.6.0
	github.com/hashicorp/go-hclog v1.0.0
	github.com/hashicorp/nomad v1.1.12
	github.com/hashicorp/nomad/api v0.0.0-20200529203653-c4416b26d3eb
	github.com/julienschmidt/httprouter v1.2.0
	github.com/labstack/echo/v4 v4.4.0
	github.com/leodido/go-urn v1.2.1 // indirect
	github.com/mitchellh/mapstructure v1.4.1
	github.com/mojocn/base64Captcha v1.3.4
	github.com/nats-io/gnatsd v1.4.1 // indirect
	github.com/nats-io/go-nats v1.7.2
	github.com/nats-io/nats-server/v2 v2.7.0
	github.com/nats-io/nats-streaming-server v0.23.2
	github.com/outbrain/golib v0.0.0-20180830062331-ab954725f502
	github.com/pingcap/dm v0.0.0-00010101000000-000000000000
	github.com/pingcap/tidb v1.1.0-beta.0.20211025024448-36e694bfc536
	github.com/pingcap/tidb/parser v0.0.0-20220216061539-43e666ba7c23
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.5.1
	github.com/satori/go.uuid v1.2.1-0.20181028125025-b2ce2384e17b
	github.com/shirou/gopsutil/v3 v3.21.11
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726
	github.com/sijms/go-ora/v2 v2.2.17
	github.com/sjjian/oracle-sql-parser v0.0.0-20211213072517-76c7fe105991
	github.com/swaggo/echo-swagger v1.1.0
	github.com/swaggo/swag v1.7.0
	github.com/thinkeridea/go-extend v1.3.2
	golang.org/x/net v0.0.0-20211112202133-69e39bad7dc2
	golang.org/x/text v0.3.7
)

replace github.com/go-mysql-org/go-mysql => github.com/ffffwh/go-mysql v0.0.0-20211206100736-edbdc58f729a

//replace github.com/Sirupsen/logrus => github.com/sirupsen/logrus v1.4.2

replace github.com/araddon/qlbridge => github.com/ffffwh/qlbridge v0.0.0-20220113095321-0b48c80b13e9

replace github.com/pingcap/dm => github.com/actiontech/dm v0.0.0-20211206092524-9e640f6da0ac

replace github.com/pingcap/tidb => github.com/actiontech/tidb v0.0.0-20220408105833-d696979cd433

replace github.com/pingcap/tidb/parser => github.com/actiontech/tidb/parser v0.0.0-20220408105833-d696979cd433

// fix tencentcloud-sdk-go version. Try remove this line after updating nomad.
replace github.com/hashicorp/go-discover => github.com/hashicorp/go-discover v0.0.0-20211203145537-8b3ddf4349a8

// from pingcap/tidb go.mod
replace google.golang.org/grpc => google.golang.org/grpc v1.29.1
