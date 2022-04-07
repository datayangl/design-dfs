package design.dfs.namenode.namenode.datanode;

import design.dfs.common.FileInfo;
import design.dfs.common.utils.DateUtil;
import design.dfs.common.utils.DefaultScheduler;
import design.dfs.model.datanode.HeartbeatRequest;
import design.dfs.model.datanode.RegisterRequest;
import design.dfs.namenode.namenode.config.NameNodeConfig;
import design.dfs.namenode.namenode.fs.DiskFileSystem;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 管理 DataNode
 * File -> DataNode
 * DataNode -> File
 *
 * DataNode 宕机后的处理机制
 */
@Slf4j
public class DataNodeManager {
    private final Map<String, DataNodeInfo> dataNodes = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock replicaLock = new ReentrantReadWriteLock();

    /**
     * <pre>
     * 每个文件对应存储的Datanode信息
     * 目前没有对文件进行分片处理，todo
     * 比如文件aaa.png，存储在datanode01、datanode02
     *
     *    aaa.png : [
     *        datanode01,
     *        datanode02
     *    ]
     * </pre>
     */
    private final Map<String, List<DataNodeInfo>> replicaByFilename = new ConcurrentHashMap<>();

    /**
     * <pre>
     * 每个DataNode 存储的文件列表
     *
     * 比如datanode01存储有文件：aaa.jpg、bbb.jpg
     *
     *    datanode01 : [
     *        aaa.jpg,
     *        bbb.jpg
     *    ]
     * </pre>
     */
    private final Map<String, Map<String, FileInfo>> filesByDataNode = new ConcurrentHashMap<>();
    private final NameNodeConfig nameNodeConfig;
    private DiskFileSystem diskFileSystem;

    public DataNodeManager(NameNodeConfig nameNodeConfig, DefaultScheduler defaultScheduler) {
        this.nameNodeConfig = nameNodeConfig;
        long dataNodeAliveThreshold = nameNodeConfig.getDataNodeAliveCheckInterval();
        defaultScheduler.schedule("DataNode存活检测", new DataNodeAliveMonitor(),
                dataNodeAliveThreshold, dataNodeAliveThreshold, TimeUnit.MILLISECONDS);
    }

    /**
     * dataNode是否存活的监控线程
     *
     * <pre>
     *     这里存在一种情况，假设一个DataNode宕机了，从DataNode集合中摘除
     *
     * </pre>
     */
    private class DataNodeAliveMonitor implements Runnable {
        @Override
        public void run() {
            Iterator<DataNodeInfo> iterator = dataNodes.values().iterator();
            List<DataNodeInfo> toRemoveDataNode = new ArrayList<>();
            while (iterator.hasNext()) {
                DataNodeInfo next = iterator.next();
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis < next.getLatestHeartbeatTime()) {
                    continue;
                }
                log.info("DataNode存活检测超时，被移除：[hostname={}, current={}, nodeLatestHeartbeatTime={}]",
                        next, DateUtil.format(new Date(currentTimeMillis)), DateUtil.format(new Date(next.getLatestHeartbeatTime())));
                iterator.remove();
                toRemoveDataNode.add(next);
            }
            for (DataNodeInfo info : toRemoveDataNode) {
                createLostReplicaTask(info);
            }
        }
    }

    public void setDiskFileSystem(DiskFileSystem diskFileSystem) {
        this.diskFileSystem = diskFileSystem;
    }

    /**
     * 注册 datanode
     *
     * @return
     */
    public boolean register(RegisterRequest request) {
        if (dataNodes.containsKey(request.getHostname())) {
            log.info("DataNode 已注册：[hostname={}]", request.getHostname());

            return false;
        }

        DataNodeInfo dataNodeInfo = new DataNodeInfo(request.getHostname(), request.getNioPort(), request.getHttpPort(),
                System.currentTimeMillis() + nameNodeConfig.getDataNodeHeartbeatTimeout());

        dataNodeInfo.setStoredDataSize(request.getStoredDataSize());
        dataNodeInfo.setFreeSpace(request.getFreeSpace());
        dataNodeInfo.setNodeId(request.getNodeId());

        log.info("收到DataNode注册请求：[hostname={}, storageSize={}, freeSpace={}]",
                request.getHostname(), request.getStoredDataSize(), request.getFreeSpace());

        dataNodes.put(request.getHostname(), dataNodeInfo);
        return true;
    }

    /**
     * datanode 心跳
     *
     * @return
     */
    public boolean heartbeat(HeartbeatRequest request) {
        DataNodeInfo dataNodeInfo = dataNodes.get(request.getHostname());

        if (dataNodeInfo == null) {
            return false;
        }

        long latestHeartbeatTime = System.currentTimeMillis() + nameNodeConfig.getDataNodeHeartbeatTimeout();
        if (log.isDebugEnabled()) {
            log.debug("收到DataNode的心跳：[hostname={}, latestHeartbeatTime={}]", request.getHostname(), DateUtil.format(new Date(latestHeartbeatTime)));
        }

        dataNodeInfo.setLatestHeartbeatTime(latestHeartbeatTime);
        return true;
    }

    /**
     * 创建丢失副本的复制任务
     */
    private void createLostReplicaTask(DataNodeInfo dataNodeInfo) {
        // 获取需要复制的副本列表
        Map<String, FileInfo> filesByDataNode = removeFileByDataNode(dataNodeInfo.getHostname());
        if (filesByDataNode == null) {
            return;
        }

        for(FileInfo fileInfo : filesByDataNode.values()) {
            // 寻找可以复制副本的 DataNode

        }
    }

    /**
     * 从内存数据结构中移除DataNode的文件列表并返回
     *
     * @param hostname DataNode
     * @return 该DataNode的文件列表
     */
    public Map<String, FileInfo> removeFileByDataNode(String hostname) {
        replicaLock.writeLock().lock();
        try {
            return filesByDataNode.remove(hostname);
        } finally {
            replicaLock.writeLock().unlock();
        }
    }

    /**
     * 通过文件名选择一个可读的 DataNode
     */
    public DataNodeInfo chooseReadableDataNodeByFileName(String fileName) {
        return chooseReadableDataNodeByFileName(fileName, null);
    }

    /**
     * 通过文件名选择一个可读的 DataNode，同时删除不可读的DataNode
     */
    public DataNodeInfo chooseReadableDataNodeByFileName(String filename, DataNodeInfo toRemoveDataNode) {
        replicaLock.readLock().lock();
        try {
            List<DataNodeInfo> dataNodeInfos = replicaByFilename.get(filename);
            if(dataNodeInfos == null || dataNodeInfos.isEmpty()) {
                return null;
            }

            if (toRemoveDataNode != null) {
                dataNodeInfos.remove(toRemoveDataNode);
            }
            // 从可读 DataNode 中选择节点，目前的策略是 random 策略
            // round-robin 策略 todo
            int size = dataNodeInfos.size();
            Random random = new Random();
            int i = random.nextInt(size);
            return dataNodeInfos.get(i);
        } finally {
            replicaLock.readLock().unlock();
        }
    }


    public DataNodeInfo getDataNode(String hostname) {
        return dataNodes.get(hostname);
    }

}
