package design.dfs.datanode.server;

import design.dfs.common.FileInfo;
import lombok.Data;

import java.util.List;

/**
 * 存储信息
 */
@Data
public class StorageInfo {
    /**
     * 文件信息
     */
    private List<FileInfo> files;
    /**
     * 已用空间
     */
    private long storageSize;
    /**
     * 可用空间
     */
    private long freeSpace;

    public StorageInfo() {
        this.storageSize = 0L;
    }
}
