package design.dfs.datanode.server;

import design.dfs.common.enums.PacketType;
import design.dfs.common.network.AbstractChannelHandler;
import design.dfs.common.network.NettyPacket;
import design.dfs.common.network.RequestWrapper;
import design.dfs.common.utils.DefaultScheduler;
import design.dfs.datanode.config.DataNodeConfig;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;

/**
 * DataNode 服务端接口
 */
@Slf4j
public class DataNodeApis extends AbstractChannelHandler {
    private DataNodeConfig dataNodeConfig;
    private DefaultScheduler defaultScheduler;

    public DataNodeApis(DataNodeConfig dataNodeConfig, DefaultScheduler defaultScheduler) {
        this.dataNodeConfig = dataNodeConfig;
        this.defaultScheduler = defaultScheduler;
    }

    @Override
    protected boolean handlePackage(ChannelHandlerContext ctx, NettyPacket nettyPacket) throws Exception {
        PacketType packetType = PacketType.getEnum(nettyPacket.getPacketType());
        RequestWrapper requestWrapper = new RequestWrapper(ctx, nettyPacket);
        switch (packetType) {
           case TRANSFER_FILE:
               log.info("transfer file");
                break;
            default:
                break;
        }
        return true;
    }

    @Override
    protected Set<Integer> interestPackageTypes() {
        return new HashSet<>();
    }
}
