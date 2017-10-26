## v2.17.08.0 [unreleased]

### Release Notes

### Features

### Bugfixes

## v2.17.08.0 [2017-09-04]

### Release Notes

该版本是一个更新版本，包含一些性能上的优化，以及完善功能细节

### Features

### Bugfixes

## v2.17.07.0 [2017-08-03]

### Release Notes

Udup v2.17.07.0作为发布的第一个通用版本，现在支持两种复制模式: 数据同步和数据迁移，以及包含许多关键的功能，包括：

### Features

- 集群管理
    - 提供高可用、易扩展的平台化管理
    - 支持多个数据中心和区域
- 安装部署
    - 支持多种方式安装，可轻松部署和配置，包括单机与集群模式
- 复制
    - 支持MySQL（5.6,5.7）
    - 支持汇聚、扩散等多种拓扑
    - 低延迟复制，压缩传输
 - 过滤器
    - 支持库表级别过滤规则
 - 监控

### Bugfixes

- [#106](http://10.186.18.21/universe/udup/issues/106): 增量，ALTER TABLE tbl_name ADD [COLUMN] (col_name column_definition,...)解析报错.
- [#95](http://10.186.18.21/universe/udup/issues/95): 增量，insert 类型为time(1)、time(3)、time(5)、time(6),数据不一致.
- [#92](http://10.186.18.21/universe/udup/issues/92): 整型（int、tinyint、smallint、mediumint、bigint）+unsigned，insert最大边界值报错.
- [#91](http://10.186.18.21/universe/udup/issues/91): 增量，insert null，报错（eg：date、year、datetime、timetamp、tinyint）.
- [#88](http://10.186.18.21/universe/udup/issues/88): 增量，bit类型，解析出错.
- [#87](http://10.186.18.21/universe/udup/issues/87): 增量，字符集为gbk的表，insert中文字符报错.
- [#86](http://10.186.18.21/universe/udup/issues/86): 增量，特殊字符乱码.
- [#83](http://10.186.18.21/universe/udup/issues/83): 增量，year类型插入特殊字符串“00”，报错.
- [#67](http://10.186.18.21/universe/udup/issues/67): HTTP API状态接口中, 暴露 源端/目标端的队列的使用状态.
- [#57](http://10.186.18.21/universe/udup/issues/57): 全量，无主键的表，中断继续，数据重复.
- [#45](http://10.186.18.21/universe/udup/issues/45): 增加监控项, 监控数据发送是由于超时而引发或是由于包大小满而引发.
- [#44](http://10.186.18.21/universe/udup/issues/44): 源端, 允许defaultMsgBytes可配置.
- [#42](http://10.186.18.21/universe/udup/issues/42): 全量无法复制带外键关系的表.
- [#39](http://10.186.18.21/universe/udup/issues/39): 监控metric使用不便.
- [#36](http://10.186.18.21/universe/udup/issues/36): 任务状态, 缺少统计信息, 缺少报错信息.
- [#35](http://10.186.18.21/universe/udup/issues/35): 日志改进.
- [#30](http://10.186.18.21/universe/udup/issues/30): 全量后精度不一致.
- [#28](http://10.186.18.21/universe/udup/issues/28): 全量，数据中包含特殊字符“`”，报错.
- [#27](http://10.186.18.21/universe/udup/issues/27): 全量，数据中包含转译字符，报错.
- [#26](http://10.186.18.21/universe/udup/issues/26): insert空值，报错.
- [#22](http://10.186.18.21/universe/udup/issues/22): master断网，网络恢复后，无法继续复制.
- [#19](http://10.186.18.21/universe/udup/issues/19): 汇聚场景，Foreign Keys，master与slave结果不一致.
- [#10](http://10.186.18.21/universe/udup/issues/10): 管理命令，删除任务语句失效.
- [#8](http://10.186.18.21/universe/udup/issues/8): 同表汇聚，目标节点多次回放create index语句，udup报错.
- [#5](http://10.186.18.21/universe/udup/issues/5): rpm打包应支持relocation.
- [#2](http://10.186.18.21/universe/udup/issues/2): 源库有修改密码的话启动时会报错.
- [#1](http://10.186.18.21/universe/udup/issues/1): Add support for MySQL GTIDs.

This is the initial release of Udup.
