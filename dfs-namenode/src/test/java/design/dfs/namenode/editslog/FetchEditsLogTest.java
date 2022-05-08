package design.dfs.namenode.editslog;

import design.dfs.common.enums.FsOpType;
import design.dfs.namenode.config.NameNodeConfig;
import design.dfs.namenode.datanode.DataNodeManager;
import design.dfs.namenode.fs.DiskFileSystem;
import design.dfs.namenode.fs.EditLogBufferFetcher;
import design.dfs.namenode.fs.FileSystem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FetchEditsLogTest {
    private NameNodeConfig nameNodeConfig;
    private String testDir = "/tmp/dfs";

    @Before
    public void before() {
        nameNodeConfig = NameNodeConfig.builder()
                .baseDir(testDir)
                .editLogFlushThreshold(512)
                .build();

        // 清空目录
        File dir = new File(testDir);
        if (dir.exists() && dir.isDirectory()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                file.delete();
            }
        }
    }

    @After
    public void after() {
        File dir = new File(testDir);
        if (dir.exists() && dir.isDirectory()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                file.delete();
            }
        }
    }

    @Test
    public void testLogFetch() throws IOException {
        DiskFileSystem fileSystem = new DiskFileSystem(nameNodeConfig);

        int limit = 100;
        for (int i = 0; i < limit; i++ ) {
            fileSystem.mkdir("/tmp/kafka", new HashMap<>());
        }

        assertEquals(new EditLogBufferFetcher(fileSystem, 101).fetch(0L).size(), 0);
        assertEquals(new EditLogBufferFetcher(fileSystem, 50).fetch(0L).size(), 0);
    }
}
