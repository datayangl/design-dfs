package design.dfs.client.fs;

import com.google.protobuf.InvalidProtocolBufferException;
import design.dfs.client.config.FsClientConfig;
import design.dfs.client.exception.DfsClientException;
import design.dfs.common.enums.PacketType;
import design.dfs.common.exception.RequestTimeoutException;
import design.dfs.common.network.NetClient;
import design.dfs.common.network.NettyPacket;
import design.dfs.common.network.RequestWrapper;
import design.dfs.common.utils.DefaultScheduler;
import design.dfs.model.client.MkdirRequest;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 文件系统实现
 */
@Slf4j
public class FileSystemImpl implements FileSystem{
    private FsClientConfig fsClientConfig;
    private NetClient netClient;
    private DefaultScheduler defaultScheduler;

    public FileSystemImpl(FsClientConfig fsClientConfig) {
        this.fsClientConfig = fsClientConfig;
        this.defaultScheduler = new DefaultScheduler("FSClient-Scheduler-");
        int connectRetryTime = fsClientConfig.getConnectRetryTime() > 0 ? fsClientConfig.getConnectRetryTime() : -1;
        this.netClient = new NetClient("FSClient-NameNode-" + fsClientConfig.getServer(),
                defaultScheduler,
                connectRetryTime);
    }

    /**
     * 启动文件系统
     */
    public void start() throws InterruptedException {
        this.netClient.addNettyPackageListener(this::onReceiveMessage);
        this.netClient.addNetClientFailListener(() -> {
            log.info("dfs-client检测到NameNode挂了，标记NameNode已经宕机");
        });

        this.netClient.connect(fsClientConfig.getServer(), fsClientConfig.getPort());
        this.netClient.ensureConnected();
        log.info("和NameNode建立连接成功");
    }

    /**
     *
     * @param requestWrapper
     * @throws InvalidProtocolBufferException
     */
    private void onReceiveMessage(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        PacketType packetType = PacketType.getEnum(requestWrapper.getNettyPacket().getPacketType());
        log.info("receive message, packetType=[{}]", packetType);
    }

    /**
     * 创建目录
     * @param path 目录对应的路径
     * @throws Exception
     */
    @Override
    public void mkdir(String path) throws Exception {
        mkdir(path, new HashMap<String, String>());
    }

    @Override
    public void mkdir(String path, Map<String, String> attr) throws Exception {
        MkdirRequest mkdirRequest = MkdirRequest.newBuilder()
                .setPath(path)
                .putAllAttr(attr)
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(mkdirRequest.toByteArray(), PacketType.MKDIR);
        sendSync(nettyPacket);
        log.info("创建文件夹成功：[filename={}]", path);

    }

    @Override
    public void put(String filename, File file) throws Exception {

    }

    @Override
    public void put(String filename, File file, int numOfReplica) throws Exception {

    }

    @Override
    public void put(String filename, File file, int numOfReplica, Map<String, String> attr) throws Exception {

    }

    @Override
    public void get(String filename, String absolutePath) throws Exception {

    }

    @Override
    public void remove(String filename) throws Exception {

    }

    @Override
    public Map<String, String> getAttr(String filename) throws Exception {
        return null;
    }

    @Override
    public void close() {
        this.defaultScheduler.shutdown();
        this.netClient.shutdown();
    }

    @Override
    public List<FsFile> listFile(String path) throws Exception {
        return null;
    }

    private NettyPacket sendSync(NettyPacket nettyPacket) throws DfsClientException,InterruptedException, RequestTimeoutException {
        NettyPacket resp = netClient.sendSync(nettyPacket);
        if (resp.isError()) {
            throw new DfsClientException(resp.getError());
        }
        return resp;
    }
}
