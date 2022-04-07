package design.dfs.namenode.namenode.editslog;

import design.dfs.namenode.namenode.config.NameNodeConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 管理 EditLog
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
        //this.loadEditLogInfos();
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

            // 设置刷盘标志位
            isSchedulingSync = true;
        }

        // 异步刷盘
        logSync();
    }

    /**
     * 等待正在调度的刷磁盘的操作
     */
    private void waitSchedulingSync() {
        try {
            while (isSchedulingSync) {
                wait(1000);
                // 此时就释放锁，等待一秒再次尝试获取锁，去判断
                // isSchedulingSync是否为false，就可以脱离出while循环
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
             * 在这种情况下需要等待：
             * 1. 有其他线程正在刷磁盘，但是其他线程刷的磁盘的最大txid比当前需要刷磁盘的线程id少。
             * 这通常表示：正在刷磁盘的线程不会把当前线程需要刷的数据刷到磁盘中
             */
            while (txId > syncTxid && isSyncRunning) {
                try {
                    wait(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            /*
             * 多个线程在上面等待，当前一个线程刷磁盘操作完成后，唤醒了一堆线程，此时只有一个线程获取到锁。
             * 这个线程会进行刷磁盘操作，当这个线程释放锁之后，其他被唤醒的线程会依次获取到锁。
             * 此时每个被唤醒的线程需要重新判断一次，自己要做的事情是不是被其他线程干完了
             */
            if (txId <= syncTxid) {
                return;
            }

            // 交换两块缓冲区
            editLogBuffer.setReadyToSync();

            // 记录最大的txid
            syncTxid = txId;

            // 设置当前正在同步到磁盘的标志位
            isSchedulingSync = false;

            // 唤醒哪些正在wait的线程
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
            // 同步完了磁盘之后，就会将标志位复位，再释放锁
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
}
