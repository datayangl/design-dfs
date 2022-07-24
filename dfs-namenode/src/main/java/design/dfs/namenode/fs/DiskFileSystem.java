package design.dfs.namenode.fs;

import design.dfs.backup.fs.FsImage;
import design.dfs.common.annotation.TestOnly;
import design.dfs.common.enums.FsOpType;
import design.dfs.model.backup.EditLog;
import design.dfs.namenode.config.NameNodeConfig;
import design.dfs.namenode.datanode.DataNodeManager;
import design.dfs.namenode.editslog.EditLogWrapper;
import design.dfs.namenode.editslog.FsEditLog;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

/**
 * 文件系统元数据
 */
@Slf4j
public class DiskFileSystem extends AbstractFileSystem{
    private NameNodeConfig nameNodeConfig;
    private FsEditLog editLog;

    public DiskFileSystem(NameNodeConfig nameNodeConfig,
                          DataNodeManager dataNodeManager) {
        super();
        this.nameNodeConfig = nameNodeConfig;
        this.editLog = new FsEditLog(nameNodeConfig);
        dataNodeManager.setDiskFileSystem(this);
    }

    @TestOnly
    public DiskFileSystem(NameNodeConfig nameNodeConfig) {
        super();
        this.nameNodeConfig = nameNodeConfig;
        this.editLog = new FsEditLog(nameNodeConfig);
    }


    public NameNodeConfig getNameNodeConfig() {
        return nameNodeConfig;
    }

    @Override
    public void recoveryNamespace() throws Exception {
        try {
            FsImage fsImage = scanLatestValidFsImage(nameNodeConfig.getBaseDir());
            long txId = 0L;
            if (fsImage != null) {
                txId = fsImage.getMaxTxId();
                applyFsImage(fsImage);
            }
            // 回放 editLog 文件
            this.editLog.playbackEditLog(txId, editLogWrapper -> {
                EditLog editLog = editLogWrapper.getEditLog();
                int opType = editLog.getOpType();
                if (opType == FsOpType.MKDIR.getValue()) {
                    // 这里要调用super.mkdir 回放的editLog不需要再刷磁盘
                    super.mkdir(editLog.getPath(), editLog.getAttrMap());
                } else if (opType == FsOpType.CREATE.getValue()) {
                    super.createFile(editLog.getPath(), editLog.getAttrMap());
                } else if (opType == FsOpType.DELETE.getValue()) {
                    super.deleteFile(editLog.getPath());
                }
            });
        } catch (Exception e) {
            log.info("NameNode恢复命名空间异常：", e);
            throw e;
        }
    }

    @Override
    public Node listFiles(String filename) {
        Node node = super.listFiles(filename);
        return node;
    }

    /**
     * 创建目录
     *
     * @param path 目录路径
     */
    @Override
    public void mkdir(String path, Map<String, String> attr) {
        super.mkdir(path, attr);
        this.editLog.logEdit(new EditLogWrapper(FsOpType.MKDIR.getValue(), path, attr));
        log.info("创建文件夹：{}", path);
    }
    /**
     * 创建文件
     *
     * @param filename 文件路径
     */
    @Override
    public boolean createFile(String filename, Map<String, String> attr) {
        if (!super.createFile(filename, attr)) {
            return false;
        }
        this.editLog.logEdit(new EditLogWrapper(FsOpType.CREATE.getValue(), filename, attr));
        return true;
    }

    @Override
    public boolean deleteFile(String filename) {
        if (!super.deleteFile(filename)) {
            return false;
        }
        this.editLog.logEdit(new EditLogWrapper(FsOpType.DELETE.getValue(), filename));
        log.info("删除文件：{}", filename);
        return true;
    }

    /**
     * 优雅停机
     * 强制把内存里的edits log刷入磁盘中
     */
    public void shutdown() {
        log.info("Shutdown DiskNameSystem.");
        this.editLog.flush();
    }

    /**
     * 获取EditLog
     *
     * @return editLog
     */
    public FsEditLog getEditLog() {
        return editLog;
    }
}
