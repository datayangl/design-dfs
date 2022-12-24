package design.dfs.namenode.server;

import design.dfs.TestProperties;
import design.dfs.common.utils.FileUtil;
import design.dfs.namenode.config.NameNodeConfig;
import design.dfs.namenode.fs.DiskFileSystem;
import design.dfs.namenode.server.EditLogBufferFetcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class FetchEditsLogTest {
    private NameNodeConfig nameNodeConfig;
    private String testDir = TestProperties.TEST_DIR;

    @Before
    public void before() throws IOException {
        nameNodeConfig = NameNodeConfig.builder()
                .baseDir(testDir)
                .editLogFlushThreshold(512)
                .build();
        File file = new File(testDir);
        // 清空目录
        FileUtil.deleteDirectory(file);
        file.mkdir();
    }

    @After
    public void after() throws IOException {
        File dir = new File(testDir);
        FileUtil.deleteDirectory(dir);
    }

    @Test
    public void testLogFetch() throws IOException {
        DiskFileSystem fileSystem = new DiskFileSystem(nameNodeConfig);

        int limit = 100;
        for (int i = 0; i < limit; i++ ) {
            fileSystem.mkdir("/tmp/kafka_" + i, new HashMap<>());
        }

        //fileSystem.getEditLog().flush();

        assertEquals(true, new EditLogBufferFetcher(fileSystem, 50).fetch(0L).size() >= 50);
        assertEquals(true, new EditLogBufferFetcher(fileSystem, 50).fetch(50L).size() < 50);
        assertEquals(0, new EditLogBufferFetcher(fileSystem, 100).fetch(0L).size());

        // 强制刷新
        fileSystem.getEditLog().flush();
        assertEquals(100, new EditLogBufferFetcher(fileSystem, 100).fetch(0L).size());

    }
}
