package design.dfs.backup.fs;

import design.dfs.backup.config.BackupNodeConfig;
import design.dfs.common.annotation.VisibleForTesting;
import design.dfs.common.network.file.FileTransportClient;
import design.dfs.common.utils.FileUtil;
import design.dfs.namenode.fs.FsImageClearTask;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *  FsImage 检查点
 *
 *  检查点 checkpoint 负责将内存目录树进行持久化存储到磁盘。
 *
 */
@Slf4j
public class FsImageCheckpointTask implements Runnable{
    private BackupNodeConfig backupNodeConfig;
    private InMemoryFileSystem fileSystem;
    private NameNodeClient namenodeClient;
    private FileTransportClient fileTransportClient;
    private long lastCheckpointTxId;
    private FsImageClearTask fsImageClearTask;

    @Override
    public void run() {
        log.info("BackupNode start checkpoint thread");
        try {
            if (fileSystem.isRecovering()) {
                log.info("正在恢复元数据...");
                return;
            }

            if (fileSystem.getMaxTxId() == lastCheckpointTxId) {
                log.info("EditLog和上次没有变化，不进行checkpoint: [txId={}]", lastCheckpointTxId);
                return;
            }
            FsImage fsImage = fileSystem.getFsImage();
            lastCheckpointTxId = fsImage.getMaxTxId();
            String fsImageFile = backupNodeConfig.getFsImageFile(String.valueOf(System.currentTimeMillis()));

            log.info("开始执行checkpoint操作: [maxTxId={}]", fsImage.getMaxTxId());

            // 写入FsImage文件
            doCheckpoint(fsImage, fsImageFile);

            // 上传FsImage文件
            uploadFsImage(fsImageFile);

            // 删除旧的FSImage
            namenodeClient.getDefaultScheduler().scheduleOnce("删除FSImage任务", fsImageClearTask, 0);
        } catch (Exception e) {
            log.error("FsImageCheckpointTask error:", e);
        }
    }

    /**
     * 写入 FsImage 文件
     */
    @VisibleForTesting
    public void doCheckpoint(FsImage fsImage, String path) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(fsImage.toByteArray());
        FileUtil.saveFile(path, true, buffer);
        log.info("保存FsImage文件：[file={}]", path);
    }

    public void uploadFsImage(String path) {
        try {
            log.info("开始上传fsImage文件：[file={}]", path);
            fileTransportClient.sendFile(path);
            log.info("结束上传fsImage文件：[file={}]", path);
        } catch (Exception e) {
            log.info("上传FsImage异常：", e);
        }
    }
}
