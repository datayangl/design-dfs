package design.dfs.datanode.server.locate;

/**
 * 文件寻址
 */
public interface FileLocator {
    /**
     * 根据文件名寻找本机的绝对路径
     *
     * @param filename 文件名
     * @return 本机的绝对路径
     */
    String locate(String filename);
}
