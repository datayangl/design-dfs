package design.dfs.namenode.namenode.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.Properties;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NameNodeConfig {
    /**
     * 默认的文件目录
     */
    private final String DEFAULT_BASEDIR = "";

    /**
     * 默认监听的端口号
     */
    private final int DEFAULT_PORT = 2345;
    /**
     * 默认EditLog Buffer刷磁盘的阈值
     */
    private final int DEFAULT_EDITLOG_FLUSH_THRESHOLD = 524288;
    /**
     * 默认DataNode心跳超时的阈值
     */
    private final int DEFAULT_DATANODE_HEARTBEAT_TIMEOUT = 90000;
    /**
     * 默认副本数量
     */
    private final int DEFAULT_REPLICA_NUM = 2;
    /**
     * 默认检查DataNode是否心跳超时的时间间隔
     */
    private final int DEFAULT_DATANODE_ALIVE_CHECK_INTERVAL = 30000;


    private String baseDir;
    private int port;
    private int editLogFlushThreshold;
    private long dataNodeHeartbeatTimeout;
    private int replicaNum;
    private long dataNodeAliveCheckInterval;
    private String nameNodePeerServers;
    private int nameNodeId;
    private String nameNodeLaunchMode;
    private int httpPort;
    private long nameNodeTrashCheckInterval;
    private long nameNodeTrashClearThreshold;
    private int nameNodeApiCoreSize;
    private int nameNodeApiMaximumPoolSize;
    private int nameNodeApiQueueSize;

    public static NameNodeConfig parse(Properties properties) {
        String baseDir = (String) properties.get("base.dir");
        int port = Integer.parseInt((String) properties.get("port"));
        int editLogFlushThreshold = Integer.parseInt((String) properties.get("editlogs.flush.threshold"));
        long dataNodeHeartbeatTimeout = Long.parseLong((String) properties.get("datanode.heartbeat.timeout"));
        int replicaNum = Integer.parseInt((String) properties.get("replica.num"));
        long dataNodeAliveCheckInterval = Integer.parseInt((String) properties.get("datanode.alive.check.interval"));
        String nameNodePeerServers = (String) properties.get("namenode.peer.servers");
        int nameNodeId = Integer.parseInt((String) properties.get("namenode.id"));
        String nameNodeLaunchMode = (String) properties.get("namenode.launch.mode");
        int httpPort = Integer.parseInt((String) properties.get("http.port"));
        long nameNodeTrashCheckInterval = Integer.parseInt((String) properties.get("namenode.trash.check.interval"));
        long nameNodeTrashClearThreshold = Integer.parseInt((String) properties.get("namenode.trash.clear.threshold"));
        int nameNodeApiCoreSize = Integer.parseInt((String) properties.get("namenode.api.coreSize"));
        int nameNodeApiMaximumPoolSize = Integer.parseInt((String) properties.get("namenode.api.maximumPoolSize"));
        int nameNodeApiQueueSize = Integer.parseInt((String) properties.get("namenode.api.queueSize"));
        return NameNodeConfig.builder()
                .baseDir(baseDir)
                .port(port)
                .editLogFlushThreshold(editLogFlushThreshold)
                .dataNodeHeartbeatTimeout(dataNodeHeartbeatTimeout)
                .replicaNum(replicaNum)
                .dataNodeAliveCheckInterval(dataNodeAliveCheckInterval)
                .nameNodePeerServers(nameNodePeerServers)
                .nameNodeId(nameNodeId)
                .nameNodeLaunchMode(nameNodeLaunchMode)
                .httpPort(httpPort)
                .nameNodeTrashCheckInterval(nameNodeTrashCheckInterval)
                .nameNodeTrashClearThreshold(nameNodeTrashClearThreshold)
                .nameNodeApiCoreSize(nameNodeApiCoreSize)
                .nameNodeApiMaximumPoolSize(nameNodeApiMaximumPoolSize)
                .nameNodeApiQueueSize(nameNodeApiQueueSize)
                .build();
    }

    public String getEditLogsFile(long start, long end) {
        return baseDir + File.separator + "editslog-" + start + "_" + end + ".log";
    }

//    public String getFsimageFile(String time) {
//        return baseDir + File.separator + "fsimage-" + time;
//    }
//
//    public String getSlotFile() {
//        return baseDir + File.separator + "slots.meta";
//    }
//
//    public int numOfNode() {
//        return StringUtils.isEmpty(nameNodePeerServers) ? 1 : nameNodePeerServers.split(",").length;
//    }
//
//    public String getAuthInfoFile() {
//        return baseDir + File.separator + "auth.meta";
//    }
//
//    public NameNodeLaunchMode getMode() {
//        return NameNodeLaunchMode.getEnum(nameNodeLaunchMode);
//    }
//
//    public long getClearStorageThreshold() {
//        return nameNodeTrashClearThreshold;
//    }
}
