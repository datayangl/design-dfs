package design.dfs.namenode;

import design.dfs.common.utils.DefaultScheduler;
import design.dfs.namenode.config.NameNodeConfig;
import design.dfs.namenode.datanode.DataNodeManager;
import design.dfs.namenode.fs.DiskFileSystem;
import design.dfs.namenode.server.NameNodeApis;
import design.dfs.namenode.server.NameNodeServer;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class NameNode {
    /**
     * 业务逻辑处理 API
     */
    private final NameNodeApis nameNodeApis;
    /**
     * 调度器封装
     */
    private final DefaultScheduler defaultScheduler;
    /**
     * datanode 管理器
     */
    private final DataNodeManager dataNodeManager;
    /**
     * 基于磁盘的文件系统
     */
    private final DiskFileSystem diskNameSystem;
    /**
     * NameNode 服务器
     */
    private final NameNodeServer nameNodeServer;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public NameNode(NameNodeConfig nameNodeConfig) {
        this.defaultScheduler = new DefaultScheduler("NameNode-Scheduler-");
        this.dataNodeManager = new DataNodeManager(nameNodeConfig, defaultScheduler);
        this.diskNameSystem = new DiskFileSystem(nameNodeConfig, dataNodeManager);
        this.nameNodeApis = new NameNodeApis(diskNameSystem.getNameNodeConfig(), dataNodeManager);
        this.nameNodeServer = new NameNodeServer(defaultScheduler, diskNameSystem, nameNodeApis);
    }

    public static void main(String[] args) {
        // 测试阶段，使用默认配置
        String propertiesPath = NameNode.class.getClassLoader().getResource("conf/namenode.properties").getPath();
        args= new String[]{propertiesPath};
        if (args == null || args.length == 0) {
            throw new IllegalArgumentException("配置文件不能为空");
        }
        // 1. 解析配置文件
        NameNodeConfig nameNodeConfig = null;
        try {
            Path path = Paths.get(args[0]);
            try (InputStream inputStream = Files.newInputStream(path)) {
                Properties properties = new Properties();
                properties.load(inputStream);
                nameNodeConfig = NameNodeConfig.parse(properties);
            }
            log.info("NameNode启动配置文件 : {}", path.toAbsolutePath());
        } catch (Exception e) {
            log.error("无法加载配置文件 : ", e);
            System.exit(1);
        }

        parseOption(args, nameNodeConfig);
        try {
            NameNode namenode = new NameNode(nameNodeConfig);
            // add hooker for gracefully shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(namenode::shutdown));
            namenode.start();
        } catch (Exception e) {
            log.error("启动NameNode失败：", e);
            System.exit(1);
        }

    }

    /**
     * TODO 支持从 args 配置参数覆盖配置文件参数
     * @param args
     * @param nameNodeConfig
     */
    private static void parseOption(String[] args, NameNodeConfig nameNodeConfig) {

    }

    /**
     * 启动
     *
     * @throws Exception 中断异常
     */
    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            this.diskNameSystem.recoveryNamespace();
            this.nameNodeServer.start();
        }
    }

    /**
     * 优雅停机
     */
    public void shutdown()  {
        if (started.compareAndSet(true, false)) {
            this.diskNameSystem.shutdown();
            this.nameNodeServer.shutdown();
        }
    }
}
