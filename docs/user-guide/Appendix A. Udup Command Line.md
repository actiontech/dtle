Appendix A. 命令行
===================

**udup** 命令行用法如下:

	Usage: udup <command...> [<args...>]

命令行选项如下:

**-?, -h, --help**：用于显示帮助信息

**server**：启动udup进程

**members**：查看所有manager节点状态

**node-status**：查看节点状态

**job-status**：查看任务状态

**-v, version**：打印版本信息

当你执行 udup -h 上述信息将会打印到控制台

###A.1. server 命令行选项

**-config**：指定 Udup 的配置

**-bind**：本机服务地址

**-manager**：是否开启manager模式（true/false）

**-agent**：是否开启agent模式（true/false）

**-join**：agent启动时尝试加入的地址(仅限manager模式下)

**-managers**：server启动时尝试加入的地址(仅限agent模式下)

###A.2. members 命令行选项

**members** 命令行用法如下:

	Usage: udup members [options]

**-detailed**：显示详细信息

###A.3. node-status 命令行选项

**node-status** 命令行用法如下:

	Usage: udup node-status [options] <node>

**-self**：显示本机节点信息

**-stats**：显示详细的资源使用情况统计信息

**-allocs**：显示每个节点的运行分配计数

**-verbose**：显示完整信息

###A.4. job-status 命令行选项

**job-status** 命令行用法如下:

	Usage: udup job-status [options] <job>

**-evals**：显示与Job关联的评估

**-all-allocs**：显示与Job ID匹配的所有任务分配

**-verbose**：显示完整信息
