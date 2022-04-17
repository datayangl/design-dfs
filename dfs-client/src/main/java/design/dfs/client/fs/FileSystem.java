package design.dfs.client.fs;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * 客户端文件系统接口
 */
public interface FileSystem {
    /**
     * 创建目录
     *
     * @param path 目录对应的路径
     * @throws Exception 文件不存在
     */
    void mkdir(String path) throws Exception;

    /**
     * 创建目录
     *
     * @param path 目录对应的路径
     * @param attr 文件属性
     * @throws Exception 文件不存在
     */
    void mkdir(String path, Map<String, String> attr) throws Exception;

    /**
     * 上传文件
     *
     * @param filename 服务器文件路径
     * @param file     本地文件
     * @throws Exception 文件不存在
     */
    void put(String filename, File file) throws Exception;

    /**
     * 上传文件
     *
     * @param filename     服务器文件路径
     * @param file         本地文件
     * @param numOfReplica 文件副本数量
     * @throws Exception 文件不存在
     */
    void put(String filename, File file, int numOfReplica) throws Exception;

    /**
     * 上传文件
     *
     * @param filename     服务器文件路径
     * @param file         本地文件
     * @param attr         文件属性
     * @param numOfReplica 文件副本数量
     * @throws Exception 文件不存在
     */
    void put(String filename, File file, int numOfReplica, Map<String, String> attr) throws Exception;


    /**
     * 下载文件
     *
     * @param filename     存储的文件名
     * @param absolutePath 本地路径
     * @throws Exception 文件不存在
     */
    void get(String filename, String absolutePath) throws Exception;

    /**
     * 删除文件
     *
     * @param filename 文件名
     * @throws Exception 文件不存在
     */
    void remove(String filename) throws Exception;

    /**
     * 读取文件属性
     *
     * @param filename 文件名
     * @return 文件属性
     * @throws Exception 文件不存在
     */
    Map<String, String> getAttr(String filename) throws Exception;

    /**
     * 关闭
     */
    void close();

    /**
     * 列出某个目录的文件列表
     *
     * @param path 文件目录
     * @return 文件信息
     * @throws Exception 异常
     */
    List<FsFile> listFile(String path) throws Exception;
}
