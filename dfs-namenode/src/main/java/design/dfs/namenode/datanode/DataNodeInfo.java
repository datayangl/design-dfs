package design.dfs.namenode.datanode;

import design.dfs.namenode.rebalance.RemoveReplicaTask;
import design.dfs.namenode.rebalance.ReplicaTask;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * DataNode 信息
 */
@Slf4j
@Data
public class DataNodeInfo implements Comparable<DataNodeInfo>{
    public static final int STATUS_INIT = 1;
    public static final int STATUS_READY = 2;

    private Integer nodeId;
    private String hostname;
    private int httpPort;
    private int nioPort;
    private long latestHeartbeatTime;
    private volatile long storedDataSize;
    private volatile long freeSpace;
    private int status;
    private ConcurrentLinkedQueue<ReplicaTask> replicaTasks = new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<RemoveReplicaTask> removeReplicaTasks = new ConcurrentLinkedQueue<>();


    public DataNodeInfo(String hostname, int nioPort, int httpPort, long latestHeartbeatTime) {
        this.hostname = hostname;
        this.nioPort = nioPort;
        this.httpPort = httpPort;
        this.latestHeartbeatTime = latestHeartbeatTime;
        this.storedDataSize = 0L;
        this.status = STATUS_INIT;
    }

    /**
     * 增加DataNode存储信息
     *
     * @param fileSize 文件大小
     */
    public void addStoredDataSize(long fileSize) {
        synchronized (this) {
            this.storedDataSize += fileSize;
            this.freeSpace -= fileSize;
        }
    }

    /**
     * 添加副本复制任务
     *
     * @param task 任务
     */
    public void addReplicaTask(ReplicaTask task) {
        replicaTasks.add(task);
    }

    /**
     * 获取副本复制任务
     *
     * @return task任务
     */
    public List<ReplicaTask> pollReplicaTask(int maxNum) {
        List<ReplicaTask> result = new LinkedList<>();

        for (int i = 0; i < maxNum; i++) {
            ReplicaTask task = replicaTasks.poll();
            if (task == null) {
                break;
            }
            result.add(task);
        }
        return result;
    }

    public List<RemoveReplicaTask> pollRemoveReplicaTask(int maxNum) {
        List<RemoveReplicaTask> result = new LinkedList<>();

        for (int i = 0; i < maxNum; i++) {
            RemoveReplicaTask task = removeReplicaTasks.poll();
            if (task == null) {
                break;
            }
            result.add(task);
        }
        return result;
    }

    public void addRemoveReplicaTask(RemoveReplicaTask task) {
        removeReplicaTasks.add(task);
    }

    @Override
    public int compareTo(DataNodeInfo o) {
        if (this.storedDataSize > o.getStoredDataSize()) {
            return 1;
        }else if (this.storedDataSize< o.getStoredDataSize()) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataNodeInfo that = (DataNodeInfo) o;
        return nioPort == that.nioPort &&
                Objects.equals(hostname, that.hostname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostname, nioPort);
    }

    @Override
    public String toString() {
        return "DataNodeInfo{" +
                "hostname='" + hostname + '\'' +
                ", port=" + nioPort +
                '}';
    }
}
