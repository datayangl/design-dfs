package design.dfs.backup.fs;

import com.google.protobuf.InvalidProtocolBufferException;
import design.dfs.backup.config.BackupNodeConfig;
import design.dfs.common.enums.PacketType;
import design.dfs.common.exception.RequestTimeoutException;
import design.dfs.common.network.NetClient;
import design.dfs.common.network.NettyPacket;
import design.dfs.common.utils.DefaultScheduler;
import design.dfs.model.backup.EditLog;
import design.dfs.model.backup.FetchEditsLogRequest;
import design.dfs.model.backup.FetchEditsLogResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 和 NameNode 通讯的客户端
 */
@Slf4j
public class NameNodeClient {
    private final DefaultScheduler defaultScheduler;
    private final BackupNodeConfig backupnodeConfig;
    private final NetClient netClient;
    private final InMemoryFileSystem fileSystem;
    private volatile boolean shutdown = false;

    public NameNodeClient(DefaultScheduler defaultScheduler, BackupNodeConfig backupnodeConfig, InMemoryFileSystem fileSystem) {
        this.netClient = new NetClient("BackupNode-NameNode-" + backupnodeConfig.getNameNodeHostname(), defaultScheduler, 3);
        this.defaultScheduler = defaultScheduler;
        this.fileSystem = fileSystem;
        this.backupnodeConfig = backupnodeConfig;
    }

    public void start() {
        this.netClient.addConnectListener(connected -> {
            if (connected) {
                log.info("namenode connected");
            }
        });
        this.netClient.connect(backupnodeConfig.getNameNodeHostname(), backupnodeConfig.getNameNodePort());
        EditsLogFetcher editsLogFetcher = new EditsLogFetcher(backupnodeConfig, this, fileSystem);
        defaultScheduler.schedule("fetch edit log", editsLogFetcher,
                backupnodeConfig.getFetchEditLogInterval(), backupnodeConfig.getFetchEditLogInterval(), TimeUnit.MILLISECONDS);
    }

    public List<EditLog> fetchEditLog(long txId) throws RequestTimeoutException, InterruptedException, InvalidProtocolBufferException {
        FetchEditsLogRequest request = FetchEditsLogRequest.newBuilder()
                .setTxId(txId)
                .build();

        NettyPacket req = NettyPacket.buildPacket(request.toByteArray(), PacketType.FETCH_EDIT_LOG);
        NettyPacket nettyPacket = netClient.sendSync(req);
        FetchEditsLogResponse response = FetchEditsLogResponse.parseFrom(nettyPacket.getBody());
        return response.getEditLogsList();
    }

    public DefaultScheduler getDefaultScheduler() {
        return defaultScheduler;
    }

    public void shutdown() {
        this.netClient.shutdown();
    }
}
