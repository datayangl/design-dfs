package design.dfs.namenode.editslog;

import design.dfs.namenode.config.NameNodeConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 管理 EditLog
 *
 * 使用 DoubleBuffer 的并发问题：
 *  1.多个线程写 editlog == 加锁
 *  2.多个线程等待flush，这个场景是 thread1 准备flush currentBuffer(写满)，但是thread2 syncBuffer（刷盘中，比较慢）flush还未结束，此时就会导致thread1阻塞等待 thread2 flush 结束
 *
 */
@Slf4j
public class FsEditLog {
    private static Pattern indexPattern = Pattern.compile("(\\d+)_(\\d+)");

    private NameNodeConfig nameNodeConfig;

    /**
     * 每条editLog的id，自增
     */
    private volatile long txIdSeq = 0;

    /**
     * 双缓冲
     */
    private DoubleBuffer editLogBuffer;

    /**
     * 每个线程保存的txid
     * 多线程场景下，在刷写磁盘阶段可能会存在其他的线程准备写入 editlog 且 Txid < syncTxid，阻塞等待刷盘结束
     */
    private ThreadLocal<Long> localTxId = new ThreadLocal<>();

    /**
     * 当前刷新磁盘最大的txId
     */
    private volatile long syncTxid = 0;

    /**
     * 当前是否在刷磁盘
     */
    private volatile boolean isSyncRunning = false;

    /**
     * 是否正在调度一次刷盘的操作
     */
    private volatile Boolean isSchedulingSync = false;

    /**
     * 磁盘中的editLog文件, 升序
     */
    private List<EditsLogInfo> editLogInfos = null;

    public FsEditLog(NameNodeConfig nameNodeConfig) {
        this.nameNodeConfig = nameNodeConfig;
        this.editLogBuffer = new DoubleBuffer(nameNodeConfig);
        this.loadEditLogInfos();
    }

    /**
     * 写入一条editlog
     *
     * @param editLog 内容
     */
    public void logEdit(EditLogWrapper editLog) {
        synchronized (this) {
            // 等待刷盘任务结束
            waitSchedulingSync();

            txIdSeq++;
            long txId = txIdSeq;
            localTxId.set(txId);

            editLog.setTxId(txId);
            // 写入缓冲区
            try {
                editLogBuffer.write(editLog);
            } catch (IOException e) {
                log.error("写入缓冲区失败：{}", e.getMessage());
            }

            if (!editLogBuffer.shouldForceSync()) {
                return;
            }

            // 设置刷盘标志位，阻塞后续写入
            isSchedulingSync = true;
        }

        // 运行到这里意味着 isSchedulingSync = true，开始异步刷盘
        logSync();
    }

    /**
     * 等待正在调度的刷磁盘的操作
     */
    private void waitSchedulingSync() {
        try {
            while (isSchedulingSync) {
                wait(1000);
            }
        } catch (Exception e) {
            log.info("waitSchedulingSync has interrupted !!");
        }
    }

    /**
     * 异步刷磁盘
     * 并发同步模型需要考虑优化 todo
     */
    private void logSync() {
        synchronized (this) {
            long txId = localTxId.get();
            localTxId.remove();
            /*
             * 并发场景可能会出现问题：
             * 1. 有其他线程正在刷磁盘，而且其他线程刷的磁盘的最大 txid 比当前需要刷磁盘线程的 txid 小。这个场景主要是因为syncBuffer flush磁盘
             * 比较慢，而currentBuffer 很快写满了也准备刷写，此时前者的 txid 会比 后者的 txid 小
             *
             */
            while (txId > syncTxid && isSyncRunning) {
                try {
                    wait(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            if (txId <= syncTxid) {
                return;
            }

            // 交换两块缓冲区
            editLogBuffer.setReadyToSync();

            // 记录最大的txid
            syncTxid = txId;

            // 设置当前正在同步到磁盘的标志位
            isSchedulingSync = false;

            // 唤醒等待flush结束的线程
            notifyAll();

            // 正在刷磁盘
            isSyncRunning = true;
        }

        try {
            EditsLogInfo editslogInfo = editLogBuffer.flush();
            if (editslogInfo != null) {
                editLogInfos.add(editslogInfo);
            }
        } catch (IOException e) {
            log.info("FSEditlog刷磁盘失败：", e);
        }

        synchronized (this) {
            isSyncRunning = false;
            notifyAll();
        }
    }

    /**
     * 强制把内存缓冲里的数据刷入磁盘中
     */
    public void flush() {
        synchronized (this) {
            try {
                editLogBuffer.setReadyToSync();
                EditsLogInfo editslogInfo = editLogBuffer.flush();
                if (editslogInfo != null) {
                    editLogInfos.add(editslogInfo);
                }
            } catch (IOException e) {
                log.error("强制刷新EditLog缓冲区到磁盘失败.", e);
            }
        }
    }

    /**
     * 从磁盘中加载 editslog 文件信息
     */
    private void loadEditLogInfos() {
        this.editLogInfos = new CopyOnWriteArrayList<>();
        File dir = new File(nameNodeConfig.getBaseDir());
        if (!dir.isDirectory()) {
            return;
        }
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            return;
        }
        for (File file : files) {
            if (!file.getName().contains("edits")) {
                continue;
            }
            long[] index = getIndexFromFileName(file.getName());
            this.editLogInfos.add(new EditsLogInfo(index[0], index[1], nameNodeConfig.getBaseDir() + File.separator + file.getName()));
        }
        this.editLogInfos.sort(null);
    }

    /**
     * 文件名提取index
     * @param name 文件名，比如 1_100.log
     * @return index 数组， 比如  [1,100]
     */
    private long[] getIndexFromFileName(String name) {
        Matcher matcher = indexPattern.matcher(name);
        long[] result = new long[2];
        if (matcher.find()) {
            result[0] = Long.parseLong(matcher.group(1));
            result[1] = Long.parseLong(matcher.group(2));
        }
        return result;
    }
}
