package design.dfs.client;

import design.dfs.client.config.FsClientConfig;
import design.dfs.client.fs.FileSystem;
import design.dfs.client.fs.FileSystemImpl;

/**
 * dfs 客户端
 */
public class FsClient {
    public static FileSystem getFileSystem(FsClientConfig fsClientConfig) throws InterruptedException {
        FileSystemImpl fileSystem = new FileSystemImpl(fsClientConfig);
        fileSystem.start();
        return fileSystem;
    }
}
