package design.dfs.backup.fs;

import design.dfs.backup.config.BackupNodeConfig;
import design.dfs.common.enums.FsOpType;
import design.dfs.model.backup.EditLog;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * EditsLog Fetch
 */
@Slf4j
public class EditsLogFetcher implements Runnable{
    /**
     * BackupNode配置
     */
    private BackupNodeConfig backupnodeConfig;

    /**
     * RPC通讯接口
     */
    private NameNodeClient nameNodeClient;
    /**
     * 文件系统
     */
    private InMemoryFileSystem fileSystem;

    public EditsLogFetcher(BackupNodeConfig backupnodeConfig, NameNodeClient nameNodeClient, InMemoryFileSystem inMemoryNameSystem) {
        this.backupnodeConfig = backupnodeConfig;
        this.nameNodeClient = nameNodeClient;
        this.fileSystem = inMemoryNameSystem;
    }

    @Override
    public void run() {
        try {
            if (fileSystem.isRecovering()) {
                log.info("正在恢复命名空间，等待...");
                Thread.sleep(1000);
                return;
            }
            List<EditLog> editLogList = nameNodeClient.fetchEditLog(fileSystem.getMaxTxId());
            if (editLogList.size() < backupnodeConfig.getFetchEditLogSize()) {
                return;
            }

            log.info("fetch edit log: [max txId={}, size={}]", fileSystem.getMaxTxId(), editLogList.size());
            for (int i=0; i< editLogList.size(); i++) {
                EditLog editLog = editLogList.get(i);
                if (editLog != null) {
                    int op = editLog.getOpType();
                    long txId = editLog.getTxId();
                    if (fileSystem.getMaxTxId() < txId) {
                        if (op == FsOpType.MKDIR.getValue()) {
                            fileSystem.mkdir(editLog.getPath(), editLog.getAttr());
                        } else if (FsOpType.CREATE.getValue() == op) {
                            fileSystem.createFile(editLog.getPath(), editLog.getAttrMap());
                        } else if (FsOpType.DELETE.getValue() == op) {
                            fileSystem.deleteFile(editLog.getPath());
                        }
                        fileSystem.setMaxTxId(txId);
                    }
                } else {
                    log.debug("EditLog is empty : {} ", editLogList);
                }
            }
        } catch (Exception e) {
            log.error("fetch edit log thread failed:", e);
        }

    }
}
