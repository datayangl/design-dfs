package design.dfs.backupnamenode.fs;

import design.dfs.backup.fs.FsImage;
import design.dfs.common.enums.NodeType;
import design.dfs.namenode.fs.FsDirectory;
import design.dfs.namenode.fs.Node;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.*;

public class FsImageTest {

    @Test
    public void testApplyFsImage() throws IOException {
        FsDirectory fsDirectory = new FsDirectory();
        String filename = "/tmp/root/test1.xml";
        String filename2 = "/tmp/root/test2.xml";

        boolean success = fsDirectory.createFile(filename, new HashMap());
        assertTrue(success);

        success = fsDirectory.createFile(filename2, new HashMap());
        assertTrue(success);

        FsImage fsImage = fsDirectory.createFsImage();
        fsImage.setMaxTxId(2L);

        FsDirectory newDirectory = new FsDirectory();
        newDirectory.applyFsImage(fsImage);

        Node tmp = newDirectory.listFiles("/tmp");
        assertNotNull(tmp);

        Node root =  tmp.getChildren().get("root");
        assertNotNull(root);
        assertEquals(root.getType(), NodeType.DIRECTORY.getValue());

        Node test1 = root.getChildren().get("test1.xml");
        assertNotNull(test1);
        assertEquals(test1.getType(), NodeType.FILE.getValue());

        Node test2 = root.getChildren().get("test2.xml");
        assertNotNull(test2);
        assertEquals(test2.getType(), NodeType.FILE.getValue());
    }

    @Test
    public void testCheckpoint() {

    }
}
