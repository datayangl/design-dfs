package design.dfs.common.network.file;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 文件接受处理器
 *
 */
@Slf4j
public class FileReceiveHandler {
    private FileTransportCallback fileTransportCallback;
    private AtomicLong lastCheckpoint;
    private Map<String, FileAppender> fileAppenderMap = new ConcurrentHashMap<>();

    public FileReceiveHandler(FileTransportCallback fileTransportCallback) {
        this.fileTransportCallback = fileTransportCallback;
        this.lastCheckpoint = new AtomicLong(System.currentTimeMillis() + 30 * 60 * 1000);
    }

    /**
     * 处理逻辑：
     * 1. FilePacket.HEAD => 创建 FileAppender 并缓存，期间会创建文件
     * 2. FilePacket.BODY => 调用 FileAppender 写入文件
     * 3. FilePacket.TAIL => 移除 FileAppender，并回调
     *
     * @param filePacket
     */
    public void handleRequest(FilePacket filePacket) {
        FileAppender fileAppender = null;
        try {
            Map<String, String> fileMetaData = filePacket.getFileMetaData();
            FileAttribute fileAttribute = new FileAttribute(fileMetaData);
            String id = fileAttribute.getId();
            if (FilePacket.HEAD == filePacket.getType()) {
                fileAppender = new FileAppender(fileAttribute, fileTransportCallback);
                fileAppenderMap.put(id, fileAppender);
            } else if (FilePacket.BODY == filePacket.getType()) {
                fileAppender = fileAppenderMap.get(id);
                byte[] fileData = filePacket.getBody();
                fileAppender.append(fileData);
            } else if (FilePacket.TAIL == filePacket.getType()) {
                fileAppender = fileAppenderMap.remove(id);
                fileAppender.completed();
            }
        } catch (Throwable e) {
            log.info("文件传输异常：", e);
        } finally {
            if (fileAppender != null && FilePacket.TAIL == filePacket.getType()) {
                fileAppender.release();
            }

            if (System.currentTimeMillis() > lastCheckpoint.getAndAdd(30 * 60 * 1000)) {
                checkFileReceiveTimeout();
            }
        }
    }

    /**
     * 定时检查，接受文件是否超时
     * <p>
     * 若超时，则打印告警日志，并释放资源
     */
    public void checkFileReceiveTimeout() {
        ArrayList<FileAppender> fileReceivers = new ArrayList<>(fileAppenderMap.values());
        for (FileAppender fileReceiver : fileReceivers) {
            if (fileReceiver.isTimeout()) {
                fileReceiver.release();
                log.warn("FileReceiver is timeout: [filename={}, length={}, readLength={}]",
                        fileReceiver.getFileAttribute().getFilename(), fileReceiver.getFileAttribute().getSize(),
                        fileReceiver.getReadLength());
            }
        }
    }
}
