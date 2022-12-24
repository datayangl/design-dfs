package design.dfs.namenode.editslog;

import design.dfs.TestProperties;
import design.dfs.common.enums.FsOpType;
import design.dfs.common.utils.FileUtil;
import design.dfs.namenode.config.NameNodeConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class FsEditLogTest {
    private final String testDir = TestProperties.TEST_DIR;

    @Before
    public void before() throws IOException {
        File file = new File(testDir);
        FileUtil.deleteDirectory(file);
        file.mkdir();
    }

    @After
    public void after() throws IOException {
        File file = new File(testDir);
        FileUtil.deleteDirectory(file);
    }

    @Test
    public void testMultiThreadLogEdit() throws InterruptedException {
        NameNodeConfig config = NameNodeConfig.builder().baseDir(testDir).editLogFlushThreshold(1024*1).build();
        FsEditLog fsEditLog = new FsEditLog(config);
        Map<String,String> attr = new HashMap<>();

        int threadNum = 10;
        int logNum = 100;

        CountDownLatch latch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; i++) {
            new Thread(() -> {
                try {
                    for (int j = 0; j < logNum; j++) {
                        String path = testDir + "_" + j + "_" + Thread.currentThread().getName();
                        fsEditLog.logEdit(new EditLogWrapper(FsOpType.MKDIR.getValue(), path, attr));
                    }
                } finally {
                    latch.countDown();
                }

            }).start();
        }
        latch.await();
    }
}
