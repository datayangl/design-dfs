package design.dfs.namenode.namenode.editslog;

import design.dfs.namenode.namenode.config.NameNodeConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;

/**
 * 双缓冲
 *
 */
@Slf4j
public class DoubleBuffer {
    private NameNodeConfig nameNodeConfig;
    private EditLogBuffer currentBuffer;
    private EditLogBuffer syncBuffer;

    public DoubleBuffer(NameNodeConfig nameNodeConfig) {
        this.nameNodeConfig = nameNodeConfig;
        this.currentBuffer = new EditLogBuffer(nameNodeConfig);
        this.syncBuffer = new EditLogBuffer(nameNodeConfig);
    }

    /**
     * 写入 editlog
     */
    public void write(EditLogWrapper editLog) throws IOException {
        currentBuffer.write(editLog);
        log.info("写入editslog:{}, 当前缓冲区大小为{}", editLog, currentBuffer.size());
    }

    /**
     * 交换两块缓冲区
     */
    public void setReadyToSync() {
        EditLogBuffer temp = currentBuffer;
        currentBuffer = syncBuffer;
        syncBuffer = temp;
    }

    /**
     * 把缓冲区的editlog数据刷新到磁盘
     */
    public EditsLogInfo flush() throws IOException {
        EditsLogInfo editslogInfo = syncBuffer.flush();
        if (editslogInfo != null) {
            syncBuffer.clear();
        }
        return editslogInfo;
    }

    /**
     * 判断是否可以刷新磁盘 (currentBuffer 的大小是否超过溢写值)
     *
     * @return
     */
    public boolean shouldForceSync() {
        return currentBuffer.size() >= nameNodeConfig.getEditLogFlushThreshold();
    }

    public List<EditLogWrapper> getCurrentEditLog() {
        return currentBuffer.getCurrentEditLog();
    }
}
