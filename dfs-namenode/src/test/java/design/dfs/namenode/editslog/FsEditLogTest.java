package design.dfs.namenode.editslog;

import design.dfs.common.enums.FsOpType;
import design.dfs.namenode.namenode.config.NameNodeConfig;
import design.dfs.namenode.namenode.editslog.DoubleBuffer;
import design.dfs.namenode.namenode.editslog.EditLogWrapper;
import design.dfs.namenode.namenode.editslog.FsEditLog;
import design.dfs.namenode.namenode.fs.DiskFileSystem;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class FsEditLogTest {
    @Test
    public void testMultiThreadLogEdit() throws InterruptedException {
        NameNodeConfig config = NameNodeConfig.builder().baseDir("/Users/luoy/Desktop/test/dfs").editLogFlushThreshold(1024*1).build();
        FsEditLog fsEditLog = new FsEditLog(config);
        Map<String,String> attr = new HashMap<>();

//        for (int i=0; i<100; i++) {
//            String path = "/tmp/f-" + i;
//            fsEditLog.logEdit(new EditLogWrapper(FsOpType.MKDIR.getValue(), path, attr));
//        }

        int threadNum = 10;
        int logNum = 100;

        CountDownLatch latch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; i++) {
            new Thread(() -> {
                try {
                    for (int j = 0; j < logNum; j++) {
                        String path = "/Users/luoy/Desktop/test/dfs/test_" + j + "_" + Thread.currentThread().getName();
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
