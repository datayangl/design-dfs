package design.dfs.datanode.namenode;

import design.dfs.common.enums.PacketType;
import design.dfs.common.network.NettyPacket;
import design.dfs.datanode.config.DataNodeConfig;
import design.dfs.model.datanode.HeartbeatRequest;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

/**
 * send heartbeat to NameNode
 */
@Slf4j
public class HeartbeatTask implements Runnable{
    private DataNodeConfig datanodeConfig;
    private ChannelHandlerContext ctx;

    public HeartbeatTask(ChannelHandlerContext ctx, DataNodeConfig datanodeConfig) {
        this.ctx = ctx;
        this.datanodeConfig = datanodeConfig;
    }

    @Override
    public void run() {
        HeartbeatRequest request = HeartbeatRequest.newBuilder()
                .setHostname(datanodeConfig.getDataNodeTransportAddr())
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.HEART_BEAT);
        ctx.writeAndFlush(nettyPacket);
    }
}
