package design.dfs.datanode;

import design.dfs.common.utils.DefaultScheduler;
import design.dfs.datanode.config.DataNodeConfig;
import design.dfs.datanode.namenode.NameNodeClient;
import design.dfs.datanode.server.DataNodeApis;
import design.dfs.datanode.server.DataNodeServer;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class DataNode {
    private DefaultScheduler defaultScheduler;
    private AtomicBoolean started = new AtomicBoolean(false);
    private NameNodeClient nameNodeClient;
    private DataNodeServer dataNodeServer;

    public static void main(String[] args) {
        // 测试阶段，使用默认配置
        String propertiesPath = DataNode.class.getClassLoader().getResource("conf/datanode.properties").getPath();
//        if (args == null || args.length == 0) {
//            throw new IllegalArgumentException("配置文件不能为空");
//        }

        // 1. 初始化配置文件
        DataNodeConfig dataNodeConfig = null;
        try {
            Path path = Paths.get(propertiesPath);
            try (InputStream inputStream = Files.newInputStream(path)) {
                Properties properties = new Properties();
                properties.load(inputStream);
                dataNodeConfig = DataNodeConfig.parse(properties);
            }
            log.info("DameNode启动配置文件: {}", path.toAbsolutePath());
        } catch (Exception e) {
            log.error("无法加载配置文件 : ", e);
            System.exit(1);
        }
        parseOption(args, dataNodeConfig);
        try {
            DataNode dataNode = new DataNode(dataNodeConfig);
            Runtime.getRuntime().addShutdownHook(new Thread(dataNode::shutdown));
            dataNode.start();
        } catch (Exception e) {
            log.error("启动DataNode失败：", e);
            System.exit(1);
        }
    }

    /**
     * TODO 支持从 args 中解析参数
     * @param args
     * @param dataNodeConfig
     */
    private static void parseOption(String[] args, DataNodeConfig dataNodeConfig) {

    }

    public DataNode(DataNodeConfig dataNodeConfig) {
        this.defaultScheduler = new DefaultScheduler("DataNode-Scheduler-");
        DataNodeApis dataNodeApis = new DataNodeApis(dataNodeConfig, defaultScheduler);
        this.nameNodeClient = new NameNodeClient(defaultScheduler, dataNodeConfig);
        this.dataNodeServer = new DataNodeServer(dataNodeConfig, defaultScheduler, dataNodeApis);
    }

    /**
     * 启动DataNode
     *
     * @throws InterruptedException 中断异常
     */
    private void start() throws InterruptedException {
        if (started.compareAndSet(false, true)) {
            this.nameNodeClient.start();
            this.dataNodeServer.start();
        }
    }

    /**
     * 优雅停止
     */
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            this.defaultScheduler.shutdown();
            this.nameNodeClient.shutdown();
            this.dataNodeServer.shutdown();
        }
    }
}
