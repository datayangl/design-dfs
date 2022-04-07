package design.dfs.namenode.namenode.fs;

import design.dfs.common.enums.FsOpType;
import design.dfs.namenode.namenode.config.NameNodeConfig;
import design.dfs.namenode.namenode.datanode.DataNodeManager;
import design.dfs.namenode.namenode.editslog.EditLogWrapper;
import design.dfs.namenode.namenode.editslog.FsEditLog;
import lombok.AllArgsConstructor;
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
//        TrashPolicyDefault trashPolicyDefault = new TrashPolicyDefault(this, dataNodeManager, userManager);
//        defaultScheduler.schedule("定时扫描物理删除文件", trashPolicyDefault,
//                nameNodeConfig.getNameNodeTrashCheckInterval(),
//                nameNodeConfig.getNameNodeTrashCheckInterval(), TimeUnit.MILLISECONDS);
    }

    public NameNodeConfig getNameNodeConfig() {
        return nameNodeConfig;
    }

    @Override
    public void recoveryNamespace() throws Exception {
        // 从快照恢复namespace todo
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
