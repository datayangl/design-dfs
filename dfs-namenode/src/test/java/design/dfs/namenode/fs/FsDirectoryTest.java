package design.dfs.namenode.fs;

import design.dfs.common.enums.NodeType;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.*;

/**
 * 文件目录树单元测试
 */
public class FsDirectoryTest {
    @Test
    public void testCreateFile() {
        FsDirectory fsDirectory = new FsDirectory();
        String filename = "/tmp/root/test.xml";

        boolean success = fsDirectory.createFile(filename, new HashMap());
        assertTrue(success);

        success = fsDirectory.createFile(filename, new HashMap());
        assertFalse(success);

        Node node = fsDirectory.listFiles("/tmp");
        assertNotNull(node);

        Node root = node.getChildren().get("root");
        assertNotNull(root);
        assertEquals(root.getType(), NodeType.DIRECTORY.getValue());

        Node fileNode = root.getChildren("test.xml");
        assertNotNull(fileNode);
        assertEquals(fileNode.getType(), NodeType.FILE.getValue());
    }

    @Test
    public void testDeleteFileTest() {
        FsDirectory fsDirectory = new FsDirectory();
        String filename = "/tmp/root/test.xml";

        boolean success = fsDirectory.createFile(filename, new HashMap());
        assertTrue(success);

        Node node = fsDirectory.listFiles(filename);
        assertNotNull(node);
        assertEquals(node.getType(), NodeType.FILE.getValue());

        Node delete = fsDirectory.delete(filename);
        assertNotNull(delete);

        Node delRet = fsDirectory.delete("/tmp");
        assertNull(delRet);

        node = fsDirectory.listFiles(filename);
        assertNull(node);
    }

    @Test
    public void testMultiWriteRead() throws InterruptedException {
        FsDirectory fsDirectory = new FsDirectory();
        int threadNum = 1000;
        Queue<String> queue = new ConcurrentLinkedQueue<>();
        for (int i=0; i < 100000; i++) {
            String parentPath = String.format("%03d", i / 1024);
            String childPath = String.format("%03d", i % 1024);
            String path = File.separator + parentPath + File.separator + childPath + File.separator + i + ".xml";
            queue.add(path);
        }

        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        for (int i = 0; i < threadNum; i++) {
            new Thread(() -> {
                String path;
                while ((path = queue.poll()) != null) {
                    fsDirectory.createFile(path, new HashMap<>());
                    Node node = fsDirectory.listFiles(path);
                    assertNotNull(node);
                }
                countDownLatch.countDown();
            }).start();
        }
        countDownLatch.await();
    }


    @Test
    public void mkdirTest() {
        FsDirectory fsDirectory = new FsDirectory();
        String filename = "/tmp/root";

        fsDirectory.mkdir(filename, new HashMap<>());

        Node node = fsDirectory.listFiles("/tmp");
        assertNotNull(node);

        Node root = node.getChildren().get("root");
        assertNotNull(root);

        assertEquals(root.getType(), NodeType.DIRECTORY.getValue());
    }


}
