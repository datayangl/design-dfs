package design.dfs.namenode.server;

import design.dfs.namenode.editslog.EditLogWrapper;
import design.dfs.namenode.editslog.EditsLogInfo;
import design.dfs.namenode.fs.DiskFileSystem;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * EditLog fetch
 */
@Slf4j
public class EditLogBufferFetcher {
    private static final int DEFAULT_FETCH_SIZE = 10;
    private int fetchSize;
    private List<EditLogWrapper> bufferedEditLog = new ArrayList<>();
    private DiskFileSystem fileSystem;

    public EditLogBufferFetcher(DiskFileSystem fileSystem) {
        this(fileSystem, DEFAULT_FETCH_SIZE);
    }

    public EditLogBufferFetcher(DiskFileSystem fileSystem, int fetchSize) {
        this.fileSystem = fileSystem;
        this.fetchSize = fetchSize;
    }

    /**
     * 判断缓冲区的大小，如果小于阈值，则尝试加载 EditLog 到缓存，如果大于阈值，则直接遍历返回
     *
     * @param txId
     * @return
     * @throws IOException
     */
    public List<EditLogWrapper> fetch(long txId) throws IOException {
        List<EditLogWrapper> result = new ArrayList<>();
        if (bufferedEditLog.size() < fetchSize) {
            fetchEditLogAppendBuffer(txId);
        }

        if (bufferedEditLog.size() >= fetchSize) {
            Iterator<EditLogWrapper> iterator = bufferedEditLog.iterator();
            while (iterator.hasNext()) {
                EditLogWrapper next = iterator.next();
                result.add(next);
                iterator.remove();
            }
        }
        return result;
    }

    /**
     * fetch EditLog 到 缓存
     * 优先从文件中 fetch EditLog，如果达不到阈值，则再从内存缓冲中 fetch
     *
     * file: (startId,endId)
     * if (txId >= endId) 说明这个 file 中全部的 EditLog 已同步，跳过
     * if (txId < endId) 说明这个 file 中 （startId,txId) EditLog 已同步，(txId,endId) EditLog 未同步
     *
     * @param txId
     * @throws IOException
     */
    private void fetchEditLogAppendBuffer(long txId) throws IOException {
        List<EditsLogInfo> sortedEditLogFiles = fileSystem.getEditLog().getSortedEditsLogFiles(txId);
        if (sortedEditLogFiles.isEmpty()) {
            appendMemoryEditLogToBuffer(txId);
        } else {
            long bufferedTxId = 0L;
            for (EditsLogInfo info : sortedEditLogFiles) {
                bufferedTxId = info.getEnd();
                if (txId > bufferedTxId) {
                    // 当前文件的 EditLog 已同步
                    continue;
                }
                List<EditLogWrapper> editsLogs = fileSystem.getEditLog().readEditLogFromFile(info.getName());
                // 缓存 EditLog
                appendInternal(txId, editsLogs);

                // 文件中获取的 EditLog 达到 Fetch 阈值，跳出循环
                if (txId + fetchSize < bufferedTxId) {
                    break;
                }
            }

            // 文件中的 EditLog 数量达不到阈值，尝试获取内存中的 EditLog
            if (bufferedTxId <= txId) {
                appendMemoryEditLogToBuffer(txId);
            }
        }
    }

    /**
     * 获取内存中的 EditLog 进行缓存
     * @param minTxId
     */
    private void appendMemoryEditLogToBuffer(long minTxId) {
        List<EditLogWrapper> currentEditLog = fileSystem.getEditLog().getCurrentEditLog();
        appendInternal(minTxId, currentEditLog);
    }

    /**
     * 缓存 EditLog (txId > minTxId)
     * @param minTxId
     * @param editLogList
     */
    private void appendInternal(long minTxId, List<EditLogWrapper> editLogList) {
        for (EditLogWrapper editLog : editLogList) {
            long txId = editLog.getTxId();
            if (txId > minTxId) {
                bufferedEditLog.add(editLog);
            }
        }
    }
}
