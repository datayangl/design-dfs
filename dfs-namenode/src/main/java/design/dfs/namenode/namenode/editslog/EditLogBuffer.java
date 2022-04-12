package design.dfs.namenode.namenode.editslog;

import design.dfs.common.utils.FileUtil;
import design.dfs.namenode.namenode.config.NameNodeConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * EditLog 缓冲
 */
@Slf4j
public class EditLogBuffer {
    private final NameNodeConfig nameNodeConfig;
    private ByteArrayOutputStream buffer;
    private volatile long startTxid = -1L;
    private volatile long endTxid = 0L;

    public EditLogBuffer(NameNodeConfig nameNodeConfig) {
        this.nameNodeConfig = nameNodeConfig;
        // 双缓冲区实现，长度*2
        this.buffer = new ByteArrayOutputStream(nameNodeConfig.getEditLogFlushThreshold() * 2);
    }

    /**
     * 写入一条 editlog 到缓冲区
     *
     * @param editLog
     * @throws IOException
     */
    public void write(EditLogWrapper editLog) throws IOException {
        if (startTxid == -1) {
            startTxid = editLog.getTxId();
        }
        endTxid = editLog.getTxId();
        buffer.write(editLog.toByteArray());
    }

    /**
     * 获取当前缓冲区的所有 editlog
     *
     * @return
     */
    public List<EditLogWrapper> getCurrentEditLog() {
        byte[] bytes = buffer.toByteArray();
        if (bytes.length == 0) {
            return new ArrayList<>();
        }
        return EditLogWrapper.parseFrom(bytes);
    }

    /**
     * 清空缓冲区
     */
    public void clear() {
        startTxid = -1;
        endTxid = -1;
        buffer.reset();
    }

    /**
     * 返回当前缓冲区大小
     */
    public Integer size() {
        return buffer.size();
    }

    /**
     * 刷盘
     */
    public EditsLogInfo flush() throws IOException {
        if (buffer.size() <= 0) {
            return null;
        }
        byte[] bytes = buffer.toByteArray();
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        String path = nameNodeConfig.getEditLogsFile(startTxid, endTxid);
        log.info("保存editslog文件：[file={}]", path);
        FileUtil.saveFile(path, false, byteBuffer);
        return new EditsLogInfo(startTxid, endTxid, path);
    }
}
