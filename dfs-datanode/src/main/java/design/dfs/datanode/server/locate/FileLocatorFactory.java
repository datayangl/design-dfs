package design.dfs.datanode.server.locate;

/**
 * 文件寻址器工厂
 */
public interface FileLocatorFactory {

    /**
     * 根据配置的类型获取文件定位器
     *
     * @param type     类型
     * @param basePath 基础目录
     * @return 文件定位器
     */
    public static FileLocator getFileLocator(String type, String basePath, int hashSize) {
        return  null;
    }

}
