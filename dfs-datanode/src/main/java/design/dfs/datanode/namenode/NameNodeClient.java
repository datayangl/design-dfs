package design.dfs.datanode.namenode;

import com.google.common.collect.Lists;
import design.dfs.common.FileInfo;
import design.dfs.common.enums.PacketType;
import design.dfs.common.network.NetClient;
import design.dfs.common.network.NettyPacket;
import design.dfs.common.network.RequestWrapper;
import design.dfs.common.utils.DefaultScheduler;
import design.dfs.datanode.config.DataNodeConfig;
import design.dfs.datanode.server.StorageInfo;
import design.dfs.datanode.server.StorageManager;
import design.dfs.model.datanode.*;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * 负责和 NameNode 通讯
 */
@Slf4j
public class NameNodeClient {
    private NetClient netClient;
    private final DefaultScheduler defaultScheduler;
    private final StorageManager storageManager;
    private final DataNodeConfig datanodeConfig;
    private ScheduledFuture<?> scheduledFuture;

    public NameNodeClient(StorageManager storageManager, DefaultScheduler defaultScheduler, DataNodeConfig datanodeConfig) {
        this.netClient = new NetClient("DataNode-NameNode-" + datanodeConfig.getNameNodeAddr(), defaultScheduler);
        this.defaultScheduler = defaultScheduler;
        this.datanodeConfig = datanodeConfig;
        this.storageManager = storageManager;
    }

    /**
     * 启动 namenode 客户端, 并增加一些监听器
     */
    public void start() {
        this.netClient.addNettyPackageListener(this::onReceiveMessage);
        this.netClient.addConnectListener(connected -> {
            if (connected) {
                log.info("start to register datanode");
                register();
            } else {
                if (scheduledFuture != null) {
                    scheduledFuture.cancel(true);
                    scheduledFuture = null;
                }
            }
        });
        // TODO 网络失败监听
        this.netClient.addNetClientFailListener(() -> {
            log.info("DataNode检测到NameNode挂了，标记NameNode已宕机");
            //backupNodeManager.markNameNodeDown();
        });

        this.netClient.connect(datanodeConfig.getNameNodeAddr(), datanodeConfig.getNameNodePort());
    }


    private void onReceiveMessage(RequestWrapper requestWrapper) throws Exception {
        PacketType packetType = PacketType.getEnum(requestWrapper.getNettyPacket().getPacketType());
        switch (packetType) {
            case DATA_NODE_REGISTER:
                handleDataNodeRegisterResponse(requestWrapper);
                break;
            case HEART_BEAT:
                handleDataNodeHeartbeatResponse(requestWrapper);
                break;
            default:
                break;
        }
    }

    private void handleDataNodeHeartbeatResponse(RequestWrapper requestWrapper) throws Exception {
        if (requestWrapper.getNettyPacket().isError()) {
            log.warn("heartbeat failed,restart register: [error={}]", requestWrapper.getNettyPacket().getError());
            register();
            return;
        }
        HeartbeatResponse heartbeatResponse = HeartbeatResponse.parseFrom(requestWrapper.getNettyPacket().getBody());
        handleHeartbeatResponse(heartbeatResponse);
    }

    private void handleDataNodeRegisterResponse(RequestWrapper requestWrapper) {
        ChannelHandlerContext ctx = requestWrapper.getCtx();
        if (scheduledFuture == null) {
            log.info("start to send heartbeat at fixed rate, interval is: [interval={}ms]", datanodeConfig.getHeartbeatInterval());
            scheduledFuture = ctx.executor().scheduleAtFixedRate(new HeartbeatTask(ctx, datanodeConfig),
                    0, datanodeConfig.getHeartbeatInterval(), TimeUnit.MILLISECONDS);
        }
        if (requestWrapper.getNettyPacket().isSuccess()) {
            StorageInfo storageInfo = storageManager.getStorageInfo();
            log.info("注册成功，发送请求到NameNode进行全量上报存储信息。[size={}]", storageInfo.getFiles().size());
            reportStorageInfo(ctx, storageInfo);
        } else {
            log.info("DataNode重启，不需要全量上报存储信息。");
        }
    }

    private void handleHeartbeatResponse(HeartbeatResponse response) {
       log.info("heartbeat response: {}", response.getResp());
    }
        /**
         * register DataNode
         *
         * @throws InterruptedException
         */
    private void register() throws InterruptedException {
        StorageInfo storageInfo = storageManager.getStorageInfo();
        RegisterRequest request = RegisterRequest.newBuilder()
                .setHostname(datanodeConfig.getDataNodeTransportAddr())
                .setNioPort(datanodeConfig.getDataNodeTransportPort())
                .setHttpPort(datanodeConfig.getDataNodeHttpPort())
                .setNodeId(datanodeConfig.getDataNodeId())
                .setFreeSpace(storageInfo.getFreeSpace())
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.DATA_NODE_REGISTER);
        log.info("DataNode register: {}", request.getHostname());
        netClient.send(nettyPacket);
    }

    private void reportStorageInfo(ChannelHandlerContext ctx, StorageInfo storageInfo) {
        List<FileInfo> files = storageInfo.getFiles();
        // 每次最多上传100个文件信息
        if (files.isEmpty()) {
            ReportCompleteStorageInfoRequest request = ReportCompleteStorageInfoRequest.newBuilder()
                    .setHostname(datanodeConfig.getDataNodeTransportAddr())
                    .setFinished(true)
                    .build();
            NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.REPORT_STORAGE_INFO);
            ctx.writeAndFlush(nettyPacket);
        } else {
            List<List<FileInfo>> partition = Lists.partition(files, 100);
            for (int i = 0; i < partition.size(); i++) {
                List<FileInfo> fileInfos = partition.get(i);
                List<FileMetaInfo> fileMetaInfos = fileInfos.stream()
                        .map(e ->
                            FileMetaInfo.newBuilder()
                                    .setFilename(e.getFileName())
                                    .setFileSize(e.getFileSize())
                                    .build())
                        .collect(Collectors.toList());
                boolean isFinish = i == partition.size() - 1;
                ReportCompleteStorageInfoRequest.Builder builder = ReportCompleteStorageInfoRequest.newBuilder()
                        .setHostname(datanodeConfig.getDataNodeTransportAddr())
                        .addAllFileInfos(fileMetaInfos)
                        .setFinished(isFinish);
                ReportCompleteStorageInfoRequest request = builder.build();
                NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.REPORT_STORAGE_INFO);
                ctx.writeAndFlush(nettyPacket);
            }
        }
    }

    /**
     * 上报文件副本信息
     *
     * @param fileName 文件名称
     * @param fileSize 文件大小
     */
    public void informReplicaReceived(String fileName, long fileSize) throws InterruptedException {
        InformReplicaReceivedRequest replicaReceivedRequest = InformReplicaReceivedRequest.newBuilder()
                .setFilename(fileName)
                .setHostname(datanodeConfig.getDataNodeTransportAddr())
                .setFileSize(fileSize)
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(replicaReceivedRequest.toByteArray(), PacketType.REPLICA_RECEIVE);
        netClient.send(nettyPacket);
    }

    /**
     * 停止服务
     */
    public void shutdown() {
        if (netClient != null) {
            netClient.shutdown();
        }
    }
}
