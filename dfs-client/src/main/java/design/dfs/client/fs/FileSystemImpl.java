package design.dfs.client.fs;

import com.google.protobuf.InvalidProtocolBufferException;
import design.dfs.client.config.FsClientConfig;
import design.dfs.client.exception.DfsClientException;
import design.dfs.client.tools.OnMultiFileProgressListener;
import design.dfs.common.Constants;
import design.dfs.common.enums.PacketType;
import design.dfs.common.exception.RequestTimeoutException;
import design.dfs.common.network.NetClient;
import design.dfs.common.network.NettyPacket;
import design.dfs.common.network.RequestWrapper;
import design.dfs.common.network.file.FileTransportClient;
import design.dfs.common.network.file.OnProgressListener;
import design.dfs.common.utils.DefaultScheduler;
import design.dfs.common.utils.StringUtil;
import design.dfs.model.client.CreateFileRequest;
import design.dfs.model.client.CreateFileResponse;
import design.dfs.model.client.MkdirRequest;
import design.dfs.model.common.DataNode;
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
        put(filename, file, -1, new HashMap<>());
    }

    @Override
    public void put(String filename, File file, int numOfReplica) throws Exception {
        put(filename, file, numOfReplica, new HashMap<>());
    }

    @Override
    public void put(String filename, File file, int numOfReplica, Map<String, String> attr) throws Exception {
        put(filename, file, numOfReplica, attr, null);
    }

    /**
     * 文件上传
     *
     * 1. NameNode 创建文件，返回可上传DataNode 列表
     * 2.向指定 DataNode 传输文件
     * @param filename
     * @param file
     * @param replicaNum
     * @param attr
     * @param listener
     * @throws Exception
     */
    public void put(String filename, File file, int replicaNum, Map<String, String> attr, OnProgressListener listener) throws Exception {
        validate(filename);
        if (replicaNum > Constants.MAX_REPLICA_NUM) {
            throw new DfsClientException("不合法的副本数量：" + replicaNum);
        }
        // 检验文件的核心属性
        for (String key : Constants.KEYS_ATTR_SET) {
            if (attr.containsKey(key)) {
                log.warn("文件属性包含关键属性：[key={}]", key);
            }
        }
        if (replicaNum > 0) {
            attr.put(Constants.ATTR_REPLICA_NUM, String.valueOf(replicaNum));
        }
        CreateFileRequest request = CreateFileRequest.newBuilder()
                .setFilename(filename)
                .setFileSize(file.length())
                .putAllAttr(attr)
                .build();
        NettyPacket nettyPacket = NettyPacket.buildPacket(request.toByteArray(), PacketType.CREATE_FILE);
        NettyPacket resp = sendSync(nettyPacket);
        CreateFileResponse response = CreateFileResponse.parseFrom(resp.getBody());
        OnMultiFileProgressListener onMultiFileProgressListener = new OnMultiFileProgressListener(listener, response.getDataNodesList().size());
        for (int i = 0; i < response.getDataNodesList().size(); i++) {
            DataNode dataNodes = response.getDataNodes(i);
            String hostname = dataNodes.getHostname();
            int port = dataNodes.getNioPort();
            NetClient netClient = new NetClient("FSClient-DataNode-" + hostname, defaultScheduler);
            FileTransportClient fileTransportClient = new FileTransportClient(netClient);
            netClient.connect(hostname, port);
            netClient.ensureConnected();
            if (log.isDebugEnabled()) {
                log.debug("开始上传文件到：[node={}:{}, filename={}]", hostname, port, filename);
            }
            fileTransportClient.sendFile(response.getRealFileName(), file.getAbsolutePath(), onMultiFileProgressListener, true);
            fileTransportClient.shutdown();
            if (log.isDebugEnabled()) {
                log.debug("完成上传文件到：[node={}:{}, filename={}]", hostname, port, filename);
            }
        }

        NettyPacket confirmRequest = NettyPacket.buildPacket(request.toByteArray(), PacketType.CREATE_FILE_CONFIRM);
        confirmRequest.setTimeoutInMs(-1);
        confirmRequest.setAck(fsClientConfig.getAck());
        sendSync(confirmRequest);
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

    /**
     * 验证文件名称合法,校验连接已经认证通过
     *
     * @param filename 文件名称
     */
    private void validate(String filename) throws Exception {
        boolean ret = StringUtil.validateFileName(filename);
        if (!ret) {
            throw new DfsClientException("不合法的文件名：" + filename);
        }
    }
}
