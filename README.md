- [项目介绍](#项目介绍)
- [整体架构](#整体架构)
- [编译\&运行](#编译运行)
- [当前进度](#当前进度)

# 项目介绍

基于儒猿项目代码的练习

[分布式海量小文件存储系统](https://gitee.com/suzhou-mopdila-information/ruyuan-dfs)

# 整体架构

![整体架构](./resources/images/dfs-%20%E6%95%B4%E4%BD%93%E6%9E%B6%E6%9E%84.png)

# 编译&运行

```shell
cd design-dfs/dfs-common
mvn protobuf:compile && mvn install
```

# 当前进度

```tree
+ common
  + network
    ++ 同步请求支持 👌
+ DataNode
  + 心跳 👌
  + Storage 👌
  + DataNodeServer
    + 读写请求
+ NameNode
  + 心跳 👌
  + BackupNameNode
    + fedits 
    + fsimage
      + create 👌
      + apply 👌
      + validate
    + checkpoint
      + checkpoint task 
  + metadata
    + double buffer
  + network
    + chunck support
  + DataNode
    + peerdatanode
    + reblance
```
