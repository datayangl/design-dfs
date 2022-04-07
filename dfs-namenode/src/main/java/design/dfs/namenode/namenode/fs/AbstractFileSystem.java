package design.dfs.namenode.namenode.fs;

import design.dfs.model.namenode.Metadata;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public abstract class AbstractFileSystem implements FileSystem{
    /**
     * 负责管理内存文件目录树的组件
     */
    protected FsDirectory directory;

    public AbstractFileSystem() {
        this.directory = new FsDirectory();
    }

    /**
     * 基于本地文件恢复元数据空间
     *
     * @throws Exception IO异常
     */
    protected abstract void recoveryNamespace() throws Exception;

    @Override
    public void mkdir(String path, Map<String, String> attr) {
        this.directory.mkdir(path, attr);
    }

    @Override
    public boolean createFile(String filename, Map<String, String> attr) {
        return this.directory.createFile(filename, attr);
    }

    @Override
    public boolean deleteFile(String filename) {
        Node node = this.directory.delete(filename);
        return node != null;
    }

    @Override
    public Set<Metadata> getFilesBySlot(int slot) {
        return null;
    }

    /**
     * 获取文件列表
     *
     * @param filename 文件路径
     * @return 文件列表
     */
    public Node listFiles(String filename, int level) {
        return this.directory.listFiles(filename, level);
    }

    /**
     * 获取文件列表
     *
     * @param filename 文件路径
     * @return 文件列表
     */
    public Node listFiles(String filename) {
        return this.directory.listFiles(filename);
    }

    /**
     * <pre>
     *     假设存在文件：
     *
     *     /aaa/bbb/c1.png
     *     /aaa/bbb/c2.png
     *     /bbb/ccc/c3.png
     *
     * 传入：/aaa，则返回：[/bbb/c1.png, /bbb/c2.png]
     *
     * </pre>
     * <p>
     * 返回文件名
     */
    public List<String> findAllFiles(String path) {
        return this.directory.findAllFiles(path);
    }

    /**
     * 获取文件列表
     *
     * @param filename 文件路径
     * @return 文件列表
     */
    public Node unsafeListFiles(String filename) {
        return this.directory.unsafeListFiles(filename);
    }

    /**
     * 获取文件属性
     *
     * @param filename 文件名称
     * @return 文件属性
     */
    public Map<String, String> getAttr(String filename) {
        Node node = this.directory.listFiles(filename);
        if (node == null) {
            return null;
        }
        return Collections.unmodifiableMap(node.getAttr());
    }
}
