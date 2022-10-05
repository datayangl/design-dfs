package design.dfs.client;

import design.dfs.client.config.FsClientConfig;
import design.dfs.client.fs.FileSystem;
import design.dfs.common.network.NetClient;
import design.dfs.common.utils.DefaultScheduler;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.File;
import java.text.SimpleDateFormat;

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

    @Test
    public void testCreateFile() throws Exception {
        FileSystem fileSystem = getFileSystem();
        fileSystem.put("/tmp/t6.txt", new File("/Users/luoy/Desktop/tmp/te.txt"), 1);
        fileSystem.close();

    }


    @Test
    public void testNetClient() throws InterruptedException {
        String hostname = "localhost";
        int port = 5672;

        DefaultScheduler  defaultScheduler = new DefaultScheduler("FSClient-Scheduler-");

        NetClient netClient = new NetClient("FSClient-DataNode-" + hostname, defaultScheduler);
        netClient.connect(hostname, port);
        netClient.ensureConnected();

    }

    @Test
    public void test() {
        long now = System.currentTimeMillis();
        SimpleDateFormat format1 = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat format2 = new SimpleDateFormat("yyyy/MM/dd");
        String today = format1.format(now);
        String daybeforeyesterday1 = format1.format(now - 3600 * 1000 * 24L * 3);
        String daybeforeyesterday2 = format2.format(now - 3600 * 1000 * 24L * 3);

        System.out.println(daybeforeyesterday1);
        System.out.println(daybeforeyesterday2);

    }
}
