package design.dfs.datanode.server.locate;

import java.io.File;

/**
 * 简单路径定位器
 */
public class SimpleFileLocator extends AbstractFileLocator {
    public SimpleFileLocator(String basePath, int hashSize) {
        super(basePath, hashSize);
    }

    /**
     * "/" 替换为 "-"
     * @param filename 文件名
     * @return
     */
    @Override
    protected String encodeFileName(String filename) {
        if (filename.startsWith(File.separator)) {
            filename = filename.substring(1);
        }
        return filename.replaceAll("/", "-");
    }
}
