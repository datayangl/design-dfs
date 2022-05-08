




# 编译&运行
```
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
  + Storage
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

# 整体设计
![](resources/image/dfs-design.png)