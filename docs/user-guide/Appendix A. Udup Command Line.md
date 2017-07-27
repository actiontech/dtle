Appendix A. Udup Command Line
===================

**udup** 命令行用法如下:

	Usage: udup <command...> [<args...>]

命令行选项如下:

**-?, -h, --help**：用于显示帮助信息

**agent**：启动udup进程

**init**：初始化默认任务信息

**client-config**：查看或修改客户端配置信息

**members**：查看所有server节点状态

**node-status**：查看节点状态

**start**：启动任务

**status**：查看任务状态

**stop**：停止运行的任务

**-v, version**：打印版本信息

当你执行 udup -h 上述信息将会打印到控制台

###A.1. agent 命令行选项 

**-config**：指定 Udup 的配置

**-bind**：本机服务地址

**-server**：是否开启server模式（true/false）

**-client**：是否开启client模式（true/false）

**-join**：agent启动时尝试加入的地址(仅限server模式下)

**-servers**：agent启动时尝试加入的地址(仅限client模式下)

###A.2. start 命令行选项

**start** 命令行用法如下:

	Usage: udup <start [options] <path>


###A.3. stop 命令行选项

**stop** 命令行用法如下:

	Usage: udup <stop [options] <job>
