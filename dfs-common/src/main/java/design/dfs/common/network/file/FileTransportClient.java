package design.dfs.common.network.file;

import design.dfs.common.enums.PacketType;
import design.dfs.common.network.NetClient;
import design.dfs.common.network.NettyPacket;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 ** 文件上传、下载的客户端
 *
 * <pre>
 * 目前 FileTransportClient 主要用于文件的上传下载、FsImage 传输
 *
 * 在处理文件的上传下载的客户端中需要增加 {@link FileReceiveHandler}, 主要用于文件进度汇报
 *
 * </pre>
 */
public class FileTransportClient {
    private NetClient netClient;
    private Map<String, String> filePathMap = new ConcurrentHashMap<>();
    private Map<String, OnProgressListener> listeners = new ConcurrentHashMap<>();

    public FileTransportClient(NetClient netClient) {
        this(netClient, true);
    }

    public FileTransportClient(NetClient netClient, boolean getFile) {
        this.netClient = netClient;
        if (getFile) {
            FileTransportCallback callback = new FileTransportCallback() {
                @Override
                public String getPath(String filename) {
                    return filePathMap.remove(filename);
                }

                @Override
                public void onProgress(String filename, long total, long current, float progress, int currentWriteBytes) {
                    OnProgressListener listener = listeners.get(filename);
                    if (listener != null) {
                        listener.onProgress(total, current, progress, currentWriteBytes);
                    }
                }

                @Override
                public void onCompleted(FileAttribute fileAttribute) throws InterruptedException, IOException {
                    OnProgressListener listener = listeners.remove(fileAttribute.getFilename());
                    if (listener != null) {
                        listener.onCompleted();
                    }
                }
            };
            FileReceiveHandler fileReceiveHandler = new FileReceiveHandler(callback);
            this.netClient.addNettyPackageListener(requestWrapper -> {
                NettyPacket request = requestWrapper.getNettyPacket();
                if (request.getPacketType() == PacketType.TRANSFER_FILE.getValue()) {
                    FilePacket filePacket = FilePacket.parseFrom(requestWrapper.getNettyPacket().getBody());
                    fileReceiveHandler.handleRequest(filePacket);
                }
            });
        }
    }

    /**
     * 上传文件
     *
     * @param absolutePath 本地文件绝对路径
     * @throws Exception 文件不存在
     */
    public void sendFile(String absolutePath) throws Exception {
        sendFile(absolutePath, absolutePath, null, false);
    }

    public void sendFile(String filename, String absolutePath, OnProgressListener listener, boolean force) throws Exception {
        File file = new File(absolutePath);
        if (!file.exists()) {
            throw new FileNotFoundException("文件不存在：" + absolutePath);
        }
        DefaultFileSendTask fileSender = new DefaultFileSendTask(file, filename, netClient.socketChannel(), listener);
        fileSender.execute(force);
    }

    /**
     * 优雅关闭
     */
    public void shutdown() {
        listeners.clear();
        filePathMap.clear();
        netClient.shutdown();
    }
}
