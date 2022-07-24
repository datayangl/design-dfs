package design.dfs.datanode.server.locate;

import design.dfs.common.utils.NetUtil;
import design.dfs.common.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.File;

/**
 * 抽象文件路径定位器
 */
@Slf4j
public abstract class AbstractFileLocator implements FileLocator{
    private int hashSize;
    private String basePath;

    public AbstractFileLocator(String basePath, int hashSize) {
        this.basePath = basePath;
        this.hashSize = hashSize;
        this.encodeFileName(NetUtil.getHostName());
    }

    @Override
    public String locate(String filename) {
        String afterTransferPath = encodeFileName(filename);
        int hash = StringUtil.hash(afterTransferPath, hashSize * hashSize);
        int parent = hash / hashSize;
        int child = hash % hashSize;
        String parentPath = StringUtil.format(parent);
        String childPath = StringUtil.format(child);
        return basePath + File.separator + parentPath + File.separator + childPath + File.separator + afterTransferPath;
    }

    /**
     * 文件名转码
     *
     * @param filename 文件名
     * @return 返回文件名
     */
    protected abstract String encodeFileName(String filename);
}
