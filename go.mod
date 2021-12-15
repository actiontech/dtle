module github.com/actiontech/dtle

go 1.12

require (
	github.com/Shopify/sarama v1.26.4
	github.com/actiontech/golang-live-coverage-report v0.0.0-20210902074032-43aa91afdc2c
	github.com/alecthomas/template v0.0.0-20190718012654-fb15b899a751
	github.com/araddon/dateparse v0.0.0-20190622164848-0fb0a474d195 // indirect
	github.com/araddon/gou v0.0.0-20190110011759-c797efecbb61 // indirect
	github.com/araddon/qlbridge v0.0.0-00010101000000-000000000000
	github.com/armon/go-metrics v0.3.4
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/cznic/parser v0.0.0-20181122101858-d773202d5b1f // indirect
	github.com/cznic/strutil v0.0.0-20181122101858-275e90344537 // indirect
	github.com/cznic/y v0.0.0-20181122101901-b05e8c2e8d7b // indirect
	github.com/dchest/siphash v1.2.2 // indirect
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/docker/libkv v0.2.1
	github.com/go-playground/universal-translator v0.17.0 // indirect
	github.com/go-playground/validator v9.31.0+incompatible
	github.com/go-sql-driver/mysql v1.5.0
	github.com/godror/godror v0.24.7 // indirect
	github.com/hashicorp/go-hclog v0.14.1
	github.com/hashicorp/nomad v1.1.2
	github.com/hashicorp/nomad/api v0.0.0-20200529203653-c4416b26d3eb
	github.com/jmoiron/sqlx v1.3.4 // indirect
	github.com/julienschmidt/httprouter v1.2.0
	github.com/labstack/echo/v4 v4.2.1
	github.com/leekchan/timeutil v0.0.0-20150802142658-28917288c48d // indirect
	github.com/leodido/go-urn v1.2.1 // indirect
	github.com/lytics/datemath v0.0.0-20180727225141-3ada1c10b5de // indirect
	github.com/mb0/glob v0.0.0-20160210091149-1eb79d2de6c4 // indirect
	github.com/mitchellh/mapstructure v1.3.3
	github.com/mojocn/base64Captcha v1.3.4
	github.com/mssola/user_agent v0.5.3 // indirect
	github.com/nats-io/gnatsd v1.4.1 // indirect
	github.com/nats-io/go-nats v1.7.2
	github.com/nats-io/nats-server/v2 v2.1.6
	github.com/nats-io/nats-streaming-server v0.17.0
	github.com/outbrain/golib v0.0.0-20180830062331-ab954725f502
	github.com/pborman/uuid v1.2.1 // indirect
	github.com/pingcap/dm v0.0.0-00010101000000-000000000000
	github.com/pingcap/gofail v0.0.0-20181217135706-6a951c1e42c3 // indirect
	github.com/pingcap/parser v3.0.11+incompatible
	github.com/pingcap/tidb v0.0.0-20190613131440-c59a108c28b7
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.4.0
	github.com/remyoudompheng/bigfft v0.0.0-20190728182440-6a916e37a237 // indirect
	github.com/satori/go.uuid v1.2.0
	github.com/shirou/gopsutil v2.20.2+incompatible
	github.com/shirou/gopsutil/v3 v3.21.6-0.20210619153009-7ea8062810b6
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726
	github.com/siddontang/go-mysql v0.0.0-20200311002057-7a62847fcdb5
	github.com/sijms/go-ora/v2 v2.2.15
	github.com/sjjian/oracle-sql-parser v0.0.0-20211213072517-76c7fe105991
	github.com/stretchr/testify v1.7.0
	github.com/swaggo/echo-swagger v1.1.0
	github.com/swaggo/swag v1.7.0
	github.com/thinkeridea/go-extend v1.3.2
	golang.org/x/net v0.0.0-20201224014010-6772e930b67b
	golang.org/x/text v0.3.5
	google.golang.org/grpc v1.28.0 // indirect
	gopkg.in/go-playground/assert.v1 v1.2.1 // indirect
	gopkg.in/stretchr/testify.v1 v1.2.2 // indirect
)

replace github.com/siddontang/go-mysql => github.com/ffffwh/go-mysql v0.0.0-20210912151044-a63debbbb0a3

//replace github.com/Sirupsen/logrus => github.com/sirupsen/logrus v1.4.2

replace github.com/araddon/qlbridge => github.com/ffffwh/qlbridge v0.0.0-20181026023605-fc2d5205dad3

replace github.com/pingcap/dm => github.com/actiontech/dm v0.0.0-20201207121004-64b8d8152b3b

replace github.com/pingcap/parser => github.com/actiontech/parser v0.0.0-20211013104136-b3376f831346

replace github.com/pingcap/tidb => github.com/actiontech/tidb v0.0.0-20190910052938-5df091000377
