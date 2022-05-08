package design.dfs.backup;

import design.dfs.backup.config.BackupNodeConfig;
import design.dfs.backup.fs.InMemoryFileSystem;
import design.dfs.backup.fs.NameNodeClient;
import design.dfs.backup.server.BackupNodeServer;
import design.dfs.common.utils.DefaultScheduler;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * NameNode 备份节点
 */
@Slf4j
public class BackupNode {
    private final DefaultScheduler defaultScheduler;
    private final InMemoryFileSystem fileSystem;
    private final NameNodeClient nameNodeClient;
    private final BackupNodeServer backupNodeServer;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public static void main(String[] args) {
        if (args == null || args.length == 0) {
            throw new IllegalArgumentException("配置文件不能为空");
        }
        BackupNodeConfig backupNodeConfig = null;
        try {
            Path path = Paths.get(args[0]);
            try (InputStream inputStream = Files.newInputStream(path)) {
                Properties properties = new Properties();
                properties.load(inputStream);
                backupNodeConfig = BackupNodeConfig.parse(properties);
            }
            log.info("BackupNode 加载配置文件: [file={}].", path.toAbsolutePath().toString());
        } catch (Exception e) {
            log.error("无法加载配置文件 : ", e);
            System.exit(0);
        }
        parseOption(args, backupNodeConfig);
        try {
            BackupNode backupNode = new BackupNode(backupNodeConfig);
            backupNode.start();
            Runtime.getRuntime().addShutdownHook(new Thread(backupNode::shutdown));
        } catch (Exception e) {
            log.error("启动BackupNode失败：", e);
            System.exit(1);
        }
    }

    /**
     * TODO 支持从 args 配置参数覆盖配置文件参数
     * @param args
     * @param backupNodeConfig
     */
    private static void parseOption(String[] args, BackupNodeConfig backupNodeConfig) {

    }

    public BackupNode(BackupNodeConfig backupNodeConfig) {
        this.defaultScheduler = new DefaultScheduler("BackupNode-Scheduler-");
        this.fileSystem = new InMemoryFileSystem(backupNodeConfig);
        this.nameNodeClient = new NameNodeClient(defaultScheduler, backupNodeConfig, fileSystem);
        this.backupNodeServer = new BackupNodeServer(defaultScheduler, backupNodeConfig);
    }

    /**
     * 启动 BackupNode
     */
    private void start() throws IOException, InterruptedException {
        if (started.compareAndSet(false, true)) {
            this.fileSystem.recoveryNamespace();
            this.nameNodeClient.start();
            this.backupNodeServer.start();
        }
    }

    /**
     * 关闭
     */
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            this.defaultScheduler.shutdown();
            this.nameNodeClient.shutdown();
            this.backupNodeServer.shutdown();
        }
    }
}