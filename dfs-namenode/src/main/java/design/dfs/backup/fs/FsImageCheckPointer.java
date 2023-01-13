package design.dfs.backup.fs;

import design.dfs.backup.config.BackupNodeConfig;
import design.dfs.common.network.file.FileTransportClient;
import design.dfs.common.utils.FileUtil;
import design.dfs.namenode.fs.FsImageClearTask;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

/**
 * FsImage 检查点
 *
 * <pre>
 * 内存目录树 + txId 持久化存储到文件中，文件名为：fsimage-{时间戳}
 *
 * fsimage-1665328747554
 * fsimage-1665328757554
 * fsimage-1665328767554
 *
 * checkpoint流程：
 * 1. 获取内存目录树和 txid 生成 FsImage
 * 2. FsImage持久化
 * 3. 上传 FsImage 到 NameNode
 * 4. BackupNode 和 NameNode 清理历史数据
 *   4.1 扫描所有的FsImage文件，将文件按时间戳降序排序
 *   4.2 逐步校验FsImage文件，直到找到一个格式合法的FsImage文件
 *     4.2.1 假设上面第3个FsImage文件不合法，保存到一半的时候BackupNode宕机，或者传给NameNode的时候传了一半BackupNode宕机，导致整个文件不完整
 *     4.2.2 首先判断第3个文件，校验得出第3个不合法，删除第三个文件。继续校验第2个文件，文件合法。把第1个文件删除。只保留第2个文件
 *   4.3 基于 4.2 得到的FsImage文件，NameNode 会读取其中的TxId，然后删除比txId小的EditLogs文件
 * </pre>
 *
 */
@Slf4j
public class FsImageCheckPointer implements Runnable{
    private BackupNodeConfig backupNodeConfig;
    private InMemoryFileSystem fileSystem;
    private NameNodeClient namenodeClient;
    private FileTransportClient fileTransportClient;
    private long lastCheckpointTxId;
    private FsImageClearTask fsImageClearTask;

    public FsImageCheckPointer(NameNodeClient namenodeClient, InMemoryFileSystem fileSystem, BackupNodeConfig backupnodeConfig) {
        this.fileSystem = fileSystem;
        this.namenodeClient = namenodeClient;
        this.backupNodeConfig = backupnodeConfig;
        this.fileTransportClient = new FileTransportClient(namenodeClient.getNetClient(), false);
        this.lastCheckpointTxId = fileSystem.getMaxTxId();
        this.fsImageClearTask = new FsImageClearTask(fileSystem, backupnodeConfig.getBaseDir());
    }

    @Override
    public void run() {
        log.info("BackupNode启动checkpoint后台线程...");
        try {
            if (fileSystem.isRecovering()) {
                log.info("正在恢复元数据...");
                return;
            }
            if (fileSystem.getMaxTxId()== lastCheckpointTxId) {
                log.info("EditLog无更新，不进行checkpoint: [txId={}]", lastCheckpointTxId);
                return;
            }
            FsImage fsImage = fileSystem.getFsImage();
            lastCheckpointTxId = fsImage.getMaxTxId();
            String fsImageFile = backupNodeConfig.getFsImageFile(String.valueOf(System.currentTimeMillis()));
            // 执行checkpoint
            doCheckpoint(fsImage, fsImageFile);
            // 上传 FsImage
            uploadFsImage(fsImageFile);
            // 删除历史 FsImage
            namenodeClient.getDefaultScheduler().scheduleOnce("删除历史FSImage", fsImageClearTask, 0);
        } catch (Exception e) {
            log.error("FSImageCheckPointer error:", e);
        }
    }

    /**
     * 上传FsImage到NameNode
     */
    private void uploadFsImage(String path) {
        try {
            log.info("开始上传fsImage文件：[file={}]", path);
            fileTransportClient.sendFile(path);
            log.info("结束上传fsImage文件：[file={}]", path);
        } catch (Exception e) {
            log.info("上传FsImage异常：", e);
        }
    }

    /**
     * 写入fsImage文件
     */
    private void doCheckpoint(FsImage fsImage, String path) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(fsImage.toByteArray());
        FileUtil.saveFile(path, true, buffer);
        log.info("保存FsImage文件：[file={}]", path);
    }
}
