package design.dfs.backup.fs;

import design.dfs.backup.config.BackupNodeConfig;
import design.dfs.namenode.namenode.fs.AbstractFileSystem;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 内存文件系统
 */
@Slf4j
public class InMemoryNameSystem extends AbstractFileSystem {
    private BackupNodeConfig backupNodeConfig;
    private volatile long maxTxId = 0L;
    private AtomicBoolean recovering = new AtomicBoolean(false);


    public InMemoryNameSystem(BackupNodeConfig backupNodeConfig) {
        this.backupNodeConfig = backupNodeConfig;
    }

    public long getMaxTxId() {
        return maxTxId;
    }

    public void setMaxTxId(Long maxTxId) {
        this.maxTxId = maxTxId;
    }

    @Override
    protected void recoveryNamespace() throws IOException {
        try {
            if (recovering.compareAndSet(false, true)) {
                FsImage fsImage = scanLatestValidFsImage(backupNodeConfig.getBaseDir());
                if (fsImage != null) {
                    setMaxTxId(fsImage.getMaxTxId());

                }
                recovering.compareAndSet(true, false);
            }
        } catch (Exception e) {
            log.info("BackupNode恢复命名空间异常：", e);
            throw e;
        }
    }

}
