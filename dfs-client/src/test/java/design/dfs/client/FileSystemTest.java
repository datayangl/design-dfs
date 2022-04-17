package design.dfs.client;

import design.dfs.client.config.FsClientConfig;
import design.dfs.client.fs.FileSystem;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class FileSystemTest {
    private static FileSystem getFileSystem() throws Exception {
        FsClientConfig fsClientConfig = FsClientConfig.builder()
                .server("localhost")
                .port(5670)
                .ack(1)
                .build();
        return FsClient.getFileSystem(fsClientConfig);
    }

    @Test
    public void testMkdir() throws Exception {
        FileSystem fileSystem = getFileSystem();
        fileSystem.mkdir("/tmp/test.txt");
        fileSystem.close();
    }
}
