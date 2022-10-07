package design.dfs.datanode.server;

import design.dfs.common.enums.PacketType;
import design.dfs.common.network.AbstractChannelHandler;
import design.dfs.common.network.NettyPacket;
import design.dfs.common.network.RequestWrapper;
import design.dfs.common.network.file.FilePacket;
import design.dfs.common.network.file.FileReceiveHandler;
import design.dfs.common.utils.DefaultScheduler;
import design.dfs.datanode.config.DataNodeConfig;
import design.dfs.datanode.namenode.NameNodeClient;
import design.dfs.datanode.replica.PeerDataNodes;
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
    private FileReceiveHandler fileReceiveHandler;
    private DefaultFileTransportCallback transportCallback;
    private PeerDataNodes peerDataNodes;

    public DataNodeApis(DataNodeConfig dataNodeConfig, DefaultScheduler defaultScheduler, DefaultFileTransportCallback transportCallback) {
        this.dataNodeConfig = dataNodeConfig;
        this.defaultScheduler = defaultScheduler;
        this.fileReceiveHandler = new FileReceiveHandler(transportCallback);
        this.transportCallback = transportCallback;
    }

    public void setPeerDataNodes(PeerDataNodes peerDataNodes) {
        this.peerDataNodes = peerDataNodes;
    }

    public void setNameNodeClient(NameNodeClient nameNodeClient) {
        this.transportCallback.setNameNodeClient(nameNodeClient);
    }


    @Override
    protected boolean handlePackage(ChannelHandlerContext ctx, NettyPacket nettyPacket) throws Exception {
        PacketType packetType = PacketType.getEnum(nettyPacket.getPacketType());
        RequestWrapper requestWrapper = new RequestWrapper(ctx, nettyPacket);
        switch (packetType) {
           case TRANSFER_FILE:
                handleFileTransferRequest(requestWrapper);
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


    /**
     * 客户端上传文件处理
     */
    private void handleFileTransferRequest(RequestWrapper requestWrapper) {
        FilePacket filePacket = FilePacket.parseFrom(requestWrapper.getNettyPacket().getBody());
        if (filePacket.getType() == FilePacket.HEAD) {
        }
        fileReceiveHandler.handleRequest(filePacket);
    }
}
