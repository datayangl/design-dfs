package design.dfs.namenode.server;

import com.google.protobuf.InvalidProtocolBufferException;
import design.dfs.common.Constants;
import design.dfs.common.FileInfo;
import design.dfs.common.enums.PacketType;
import design.dfs.common.exception.NameNodeException;
import design.dfs.common.network.AbstractChannelHandler;
import design.dfs.common.network.NettyPacket;
import design.dfs.common.network.RequestWrapper;
import design.dfs.model.backup.FetchEditsLogRequest;
import design.dfs.model.backup.FetchEditsLogResponse;
import design.dfs.model.client.CreateFileRequest;
import design.dfs.model.client.CreateFileResponse;
import design.dfs.model.client.MkdirRequest;
import design.dfs.model.common.DataNode;
import design.dfs.model.datanode.FileMetaInfo;
import design.dfs.model.datanode.HeartbeatRequest;
import design.dfs.model.datanode.RegisterRequest;
import design.dfs.model.datanode.ReportCompleteStorageInfoRequest;
import design.dfs.namenode.config.NameNodeConfig;
import design.dfs.namenode.datanode.DataNodeInfo;
import design.dfs.namenode.datanode.DataNodeManager;
import design.dfs.namenode.editslog.EditLogWrapper;
import design.dfs.namenode.fs.DiskFileSystem;
import design.dfs.namenode.fs.EditLogBufferFetcher;
import design.dfs.namenode.fs.Node;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * NameNode 的业务处理逻辑
 */
@Slf4j
public class NameNodeApis extends AbstractChannelHandler {
    private final NameNodeConfig nameNodeConfig;
    private final DiskFileSystem diskFileSystem;
    private final DataNodeManager dataNodeManager;
    private final ThreadPoolExecutor executor;
    protected int nodeId;
    private final EditLogBufferFetcher editLogBufferFetcher;

    public NameNodeApis(NameNodeConfig nameNodeConfig, DiskFileSystem diskFileSystem, DataNodeManager dataNodeManager) {
        this.nameNodeConfig = nameNodeConfig;
        this.diskFileSystem = diskFileSystem;
        this.dataNodeManager = dataNodeManager;
        this.executor = new ThreadPoolExecutor(nameNodeConfig.getNameNodeApiCoreSize(), nameNodeConfig.getNameNodeApiMaximumPoolSize(),
                60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(nameNodeConfig.getNameNodeApiQueueSize()));
        this.nodeId = nameNodeConfig.getNameNodeId();
        this.editLogBufferFetcher = new EditLogBufferFetcher(diskFileSystem);
    }

    @Override
    protected Set<Integer> interestPackageTypes() {
        return new HashSet<>();
    }

    @Override
    protected Executor getExecutor() {
        return executor;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // 断开连接后的处理 todo
        log.warn("连接断开 " + ctx.channel());
    }

    /**
     * 基于 PacketType 选择相应的业务处理逻辑，类似于 kafka 的处理机制
     *
     * @param ctx     上下文
     * @param request
     * @return
     * @throws Exception
     */
    @Override
    protected boolean handlePackage(ChannelHandlerContext ctx, NettyPacket request) throws Exception {
        if (request.isError()) {
            // 在请求转发的情况下，如果目标NameNode节点发生未知异常，然后返回的结果是异常的，
            // 而源NameNode正常处理流程会出现空指针异常，从而出现死循环。
            log.warn("收到一个异常请求或响应, 丢弃不进行处理：[request={}]", request.getHeader());
            return true;
        }

        PacketType packetType = PacketType.getEnum(request.getPacketType());
        RequestWrapper requestWrapper = new RequestWrapper(ctx, request);

        try {
            switch (packetType) {
                case DATA_NODE_REGISTER:
                    handleDataNodeRegisterRequest(requestWrapper);
                    break;
                case HEART_BEAT:
                    handleDataNodeHeartbeatRequest(requestWrapper);
                    break;
                case MKDIR:
                    handleMkdirRequest(requestWrapper);
                    break;
                case FETCH_EDIT_LOG:
                    handleFetchEditLogRequest(requestWrapper);
                    break;
                case REPORT_STORAGE_INFO:
                    handleDataNodeReportStorageInfoRequest(requestWrapper);
                    break;
                case CREATE_FILE:
                    handleCreateFileRequest(requestWrapper);
                    break;
                case CREATE_FILE_CONFIRM:
                    handleCreateFileConfirmRequest(requestWrapper);
                    break;
                default:
                    break;
            }
        } catch (NameNodeException e) {
            log.error("发生业务异常：", e);
            sendErrorResponse(requestWrapper, e.getMessage());
        } catch (Exception e) {
            log.error("NameNode处理消息发生异常：", e);
            sendErrorResponse(requestWrapper, "未知异常：nodeId=" + nodeId);
        }
        return true;
    }


    private void handleDataNodeRegisterRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException, NameNodeException {
        RegisterRequest registerRequest = RegisterRequest.parseFrom(requestWrapper.getNettyPacket().getBody());
        boolean result = dataNodeManager.register(registerRequest);
        if (!result) {
            throw new NameNodeException("注册失败，DataNode节点已存在");
        }

        log.info("datanode register successfully:[request={}]", registerRequest.getNodeId());

        requestWrapper.sendResponse();
        // 广播 todo

    }

    /**
     * 处理DataNode心跳请求
     *
     * @param requestWrapper 请求
     */
    private void handleDataNodeHeartbeatRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException, NameNodeException {
        HeartbeatRequest heartbeatRequest = HeartbeatRequest.parseFrom(requestWrapper.getNettyPacket().getBody());
        boolean heartbeat = dataNodeManager.heartbeat(heartbeatRequest);
        if (!heartbeat) {
            throw new NameNodeException("心跳失败，DataNode不存在：" + heartbeatRequest.getHostname());
        }

        DataNodeInfo dataNodeInfo = dataNodeManager.getDataNode(heartbeatRequest.getHostname());

    }

    /**
     * handle mkdir request
     *
     * @param requestWrapper
     * @throws InvalidProtocolBufferException
     */
    private void handleMkdirRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        NettyPacket request = requestWrapper.getNettyPacket();
        MkdirRequest mkdirRequest = MkdirRequest.parseFrom(request.getBody());
        String fileName = mkdirRequest.getPath();
        this.diskFileSystem.mkdir(fileName, mkdirRequest.getAttrMap());
        requestWrapper.sendResponse();
    }

    private void handleFetchEditLogRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        FetchEditsLogRequest fetchEditsLogRequest = FetchEditsLogRequest.parseFrom(requestWrapper.getNettyPacket().getBody());
        long txId = fetchEditsLogRequest.getTxId();
        List<EditLogWrapper> result = new ArrayList<>();
        try {
            result = editLogBufferFetcher.fetch(txId);
        } catch (IOException e) {
            log.error("fetch EditLog failed：", e);
        }
        // 构造 response
        FetchEditsLogResponse response = FetchEditsLogResponse.newBuilder()
                .addAllEditLogs(result
                        .stream()
                        .map(EditLogWrapper::getEditLog)
                        .collect(Collectors.toList()))
                .build();
        requestWrapper.sendResponse(response);
    }

    private void handleDataNodeReportStorageInfoRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
        ReportCompleteStorageInfoRequest request =
                ReportCompleteStorageInfoRequest.parseFrom(requestWrapper.getNettyPacket().getBody());
        log.info("全量上报存储信息：[hostname={}, files={}]", request.getHostname(), request.getFileInfosCount());
        for (FileMetaInfo file : request.getFileInfosList()) {
            // TODO 考虑和内存目录树进行对比，看看是否存在内存目录树不存在的文件，下发命令让DataNode清除
            FileInfo fileInfo = new FileInfo();
            fileInfo.setFileName(file.getFilename());
            fileInfo.setFileSize(file.getFileSize());
            fileInfo.setHostname(request.getHostname());
            // TODO 增加一个副本
        }
        if (request.getFinished()) {
            dataNodeManager.setDataNodeReady(request.getHostname());
            log.info("全量上报存储信息完成：[hostname={}]", request.getHostname());
        }
    }

    private void handleCreateFileRequest(RequestWrapper requestWrapper) throws Exception {
        NettyPacket request = requestWrapper.getNettyPacket();
        CreateFileRequest createFileRequest = CreateFileRequest.parseFrom(request.getBody());
        String fileName = createFileRequest.getFilename();

        Map<String, String> attrMap = new HashMap<>(createFileRequest.getAttrMap());
        String replicaNumStr = attrMap.get(Constants.ATTR_REPLICA_NUM);
        attrMap.put(Constants.ATTR_FILE_SIZE, String.valueOf(createFileRequest.getFileSize()));
        int replicaNum;
        if (replicaNumStr != null) {
            replicaNum = Integer.parseInt(replicaNumStr);
            // 最少不能少于配置的数量
            replicaNum = Math.max(replicaNum, diskFileSystem.getNameNodeConfig().getReplicaNum());
            // 最大不能大于最大的数量
            replicaNum = Math.min(replicaNum, Constants.MAX_REPLICA_NUM);
        } else {
            replicaNum = diskFileSystem.getNameNodeConfig().getReplicaNum();
            attrMap.put(Constants.ATTR_REPLICA_NUM, String.valueOf(replicaNum));
        }
        Node node = diskFileSystem.listFiles(fileName);
        if (node != null) {
            throw new NameNodeException("文件已存在：" + createFileRequest.getFilename());
        }
        List<DataNodeInfo> dataNodeList = dataNodeManager.allocateDataNodes(request.getUserName(), replicaNum, fileName);
        List<DataNode> dataNodes = dataNodeList.stream()
                .map(e -> DataNode.newBuilder().setHostname(e.getHostname())
                        .setNioPort(e.getNioPort())
                        .setHttpPort(e.getHttpPort())
                        .build())
                .collect(Collectors.toList());
        diskFileSystem.createFile(fileName, attrMap);
        List<String> hostList = dataNodeList.stream().map(dataNodeInfo -> dataNodeInfo.getHostname()).collect(Collectors.toList());
        log.info("创建文件：[filename={}, datanodes={}]", fileName, String.join(",", hostList));
        CreateFileResponse response = CreateFileResponse.newBuilder()
                .addAllDataNodes(dataNodes)
                .setRealFileName(fileName)
                .build();
        requestWrapper.sendResponse(response);
    }

    private void handleCreateFileConfirmRequest(RequestWrapper requestWrapper) throws InvalidProtocolBufferException {
            NettyPacket request = requestWrapper.getNettyPacket();
            CreateFileRequest createFileRequest = CreateFileRequest.parseFrom(request.getBody());
            String realFileName = createFileRequest.getFilename();
            if (request.getAck() != 0) {
                log.info("文件上传完毕");
                //dataNodeManager.waitFileReceive(realFileName, 3000);
            }
            CreateFileResponse response = CreateFileResponse.newBuilder()
                    .build();
            requestWrapper.sendResponse(response);
    }
    /**
     * 返回异常响应信息
     */
    private void sendErrorResponse(RequestWrapper requestWrapper, String msg) {
        NettyPacket nettyResponse = NettyPacket.buildPacket(new byte[0],
                PacketType.getEnum(requestWrapper.getNettyPacket().getPacketType()));
        nettyResponse.setError(msg);
        requestWrapper.sendResponse(nettyResponse, requestWrapper.getRequestSequence());
    }
}
