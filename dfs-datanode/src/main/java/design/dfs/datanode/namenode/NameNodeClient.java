package design.dfs.datanode.namenode;

import design.dfs.common.enums.PacketType;
import design.dfs.common.network.NetClient;
import design.dfs.common.network.NettyPacket;
import design.dfs.common.network.RequestWrapper;
import design.dfs.common.utils.DefaultScheduler;
import design.dfs.datanode.config.DataNodeConfig;
import design.dfs.model.datanode.HeartbeatResponse;
import design.dfs.model.datanode.RegisterRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.ScheduledFuture;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

/**
 * 负责和 NameNode 通讯
 */
@Slf4j
public class NameNodeClient {
    private NetClient netClient;
    private final DefaultScheduler defaultScheduler;
    private final DataNodeConfig datanodeConfig;
    private ScheduledFuture<?> scheduledFuture;

    public NameNodeClient(DefaultScheduler defaultScheduler, DataNodeConfig datanodeConfig) {
        this.netClient = new NetClient("DataNode-NameNode-" + datanodeConfig.getNameNodeAddr(), defaultScheduler);
        this.defaultScheduler = defaultScheduler;
        this.datanodeConfig = datanodeConfig;
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
        if (scheduledFuture != null) {
            log.info("start to send heartbeat at fixed rate, interval is: [interval={}ms]", datanodeConfig.getHeartbeatInterval());
            scheduledFuture = ctx.executor().scheduleAtFixedRate(new HeartbeatTask(ctx, datanodeConfig),
                    0, datanodeConfig.getHeartbeatInterval(), TimeUnit.MILLISECONDS);
        }
        if (requestWrapper.getNettyPacket().isSuccess()) {
            log.info("register succeeded");
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
        RegisterRequest request = RegisterRequest.newBuilder()
                .setHostname(datanodeConfig.getDataNodeTransportAddr())
                .setNioPort(datanodeConfig.getNameNodePort())
                .setNodeId(datanodeConfig.getDataNodeId())
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.DATA_NODE_REGISTER);
        log.info("DataNode register: {}" + request.getHostname());
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
