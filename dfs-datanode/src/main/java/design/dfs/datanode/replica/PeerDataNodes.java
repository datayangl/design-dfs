package design.dfs.datanode.replica;

import design.dfs.common.enums.PacketType;
import design.dfs.common.network.NetClient;
import design.dfs.common.network.NettyPacket;
import design.dfs.common.utils.DefaultScheduler;
import design.dfs.datanode.config.DataNodeConfig;
import design.dfs.datanode.namenode.NameNodeClient;
import design.dfs.datanode.server.DataNodeApis;
import design.dfs.model.common.GetFileRequest;
import design.dfs.model.datanode.PeerNodeAwareRequest;
import io.netty.channel.socket.SocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
@Slf4j
public class PeerDataNodes {
    private DataNodeApis dataNodeApis;
    private DataNodeConfig dataNodeConfig;
    private DefaultScheduler defaultScheduler;
    private Map<String, PeerDataNode> dataNodeChannelMap = new ConcurrentHashMap<>();

    public PeerDataNodes(DefaultScheduler defaultScheduler, DataNodeConfig dataNodeConfig, DataNodeApis dataNodeApis) {
        this.defaultScheduler = defaultScheduler;
        this.dataNodeConfig = dataNodeConfig;
        this.dataNodeApis = dataNodeApis;
        this.dataNodeApis.setPeerDataNodes(this);
    }

    public void setNameNodeClient(NameNodeClient nameNodeClient) {
        this.dataNodeApis.setNameNodeClient(nameNodeClient);
    }
    /**
     * <pre>
     * 从目标DataNode获取文件
     *
     *  发送一个GET_FILE的网络包, 会被Peer DataNode收到, 从而把文件发送过来
     *
     *  这里有两种可能：
     *  1. datanode01和datanode02没有建立连接：
     *
     *      1.1 如果当前实例是datanode01, 会主动发起一个连接：datanode01 -> datanode02
     *      1.2 如果当前实例是datanode02, 会主动发起一个连接：datanode02 -> datanode01
     *
     *  2. 如果datanode02和datanode02已经建立连接，则会从连接池中获取到对应的连接，发送一个GET_FILE请求，从而获取文件
     *
     * </pre>
     *
     * @param hostname 主机名
     * @param port     端口号
     * @param filename 文件名
     */
    public void getFileFromPeerDataNode(String hostname, int port, String filename) throws InterruptedException {
        GetFileRequest request = GetFileRequest.newBuilder()
                .setFilename(filename)
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.GET_FILE);
        PeerDataNode peerDataNode = maybeConnectPeerDataNode(hostname, port);
        peerDataNode.send(nettyPacket);
        log.info("PeerDataNode发送GET_FILE请求，请求下载文件：[hostname={}, port={}, filename={}]", hostname, port, filename);
    }

    private PeerDataNode maybeConnectPeerDataNode(String hostname, int port) throws InterruptedException {
        synchronized (this) {
            String peerDataNode = hostname + ":" + port;
            PeerDataNode peer = dataNodeChannelMap.get(peerDataNode);
            if (peer == null) {
                NetClient netClient = new NetClient("DataNode-PeerNode-" + hostname, defaultScheduler);
                netClient.addHandlers(Collections.singletonList(dataNodeApis));
                netClient.addConnectListener(connected -> sendPeerNodeAwareRequest(netClient));
                netClient.connect(hostname, port);
                netClient.ensureConnected();
                peer = new PeerDataNodeClient(netClient, dataNodeConfig.getDataNodeId());
                dataNodeChannelMap.put(peerDataNode, peer);
            } else {
                log.info("从保存的连接中获取PeerDataNode的连接：{}", peerDataNode);
            }
            return peer;
        }

    }

    private void sendPeerNodeAwareRequest(NetClient netClient) throws InterruptedException {
        PeerNodeAwareRequest request = PeerNodeAwareRequest.newBuilder()
                .setPeerDataNode(dataNodeConfig.getDataNodeTransportServer())
                .setDataNodeId(dataNodeConfig.getDataNodeId())
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(),  PacketType.DATA_NODE_PEER_AWARE);
        log.info("尝试连接其他 PeerDataNode, 发送通知网络包：{}", request.getPeerDataNode());
        netClient.send(nettyPacket);
    }

    // --------------------------------------------------------------------------------
    //                   PeerDataNode
    // --------------------------------------------------------------------------------

    /**
     * 和一个 DataNode 节点的连接
     */
    private interface PeerDataNode {
        /**
         * 往 Peer DataNode 发送网络包, 如果连接断开了，会同步等待连接重新建立
         *
         * @param nettyPacket 网络包
         * @throws InterruptedException 中断异常
         */
        void send(NettyPacket nettyPacket) throws InterruptedException;

        /**
         * 关闭连接
         */
        void close();

        /**
         * 获取DataNodeId
         *
         * @return DataNode ID
         */
        int getDataNodeId();

        /**
         * 是否连接上
         *
         * @return 是否连接上
         */
        boolean isConnected();
    }

    private static abstract class AbstractPeerDataNode implements PeerDataNode {
        private int dataNodeId;

        public AbstractPeerDataNode(int dataNodeId) {
            this.dataNodeId = dataNodeId;
        }

        @Override
        public int getDataNodeId() {
            return dataNodeId;
        }
    }

    /**
     * 和 PeerDataNode 的连接，当前 DataNode 作为客户端
     */
    private static class PeerDataNodeClient extends AbstractPeerDataNode {
        private NetClient netClient;


        public PeerDataNodeClient(NetClient netClient, int dataNodeId) {
            super(dataNodeId);
            this.netClient = netClient;
        }

        @Override
        public void send(NettyPacket nettyPacket) throws InterruptedException {
            netClient.send(nettyPacket);
        }

        @Override
        public void close() {
            netClient.shutdown();
        }

        @Override
        public boolean isConnected() {
            return netClient.isConnected();
        }
    }

    /**
     * 表示和PeerDataNode的连接，当前DataNode作为服务端
     */
    private static class PeerDataNodeServer extends AbstractPeerDataNode {
        private volatile SocketChannel socketChannel;

        public PeerDataNodeServer(SocketChannel socketChannel, int dataNodeId) {
            super(dataNodeId);
            this.socketChannel = socketChannel;
        }

        public void setSocketChannel(SocketChannel socketChannel) {
            synchronized (this) {
                this.socketChannel = socketChannel;
                notifyAll();
            }
        }

        @Override
        public void send(NettyPacket nettyPacket) throws InterruptedException {
            synchronized (this) {
                // 如果这里断开连接了，会一直等待直到客户端会重新建立连接
                while (!socketChannel.isActive()) {
                    try {
                        wait(10);
                    } catch (InterruptedException e) {
                        log.error("PeerDataNodeServer#send has Interrupted !!");
                    }
                }
            }
            socketChannel.writeAndFlush(nettyPacket);
        }

        @Override
        public void close() {
            socketChannel.close();
        }

        @Override
        public boolean isConnected() {
            return socketChannel != null && socketChannel.isActive();
        }
    }
}
