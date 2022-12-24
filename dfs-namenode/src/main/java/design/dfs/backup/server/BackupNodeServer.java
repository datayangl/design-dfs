package design.dfs.backup.server;

import design.dfs.backup.config.BackupNodeConfig;
import design.dfs.common.network.NetServer;
import design.dfs.common.utils.DefaultScheduler;

import java.util.Collections;

/**
 * BackupNode 服务端
 */
public class BackupNodeServer {
    private BackupNodeConfig backupNodeConfig;
    private NetServer netServer;

    public BackupNodeServer(DefaultScheduler defaultScheduler, BackupNodeConfig backupNodeConfig) {
        this.netServer = new NetServer("BackupNode-Server", defaultScheduler);
        this.backupNodeConfig = backupNodeConfig;
    }

    /**
     * 启动并绑定端口
     *
     * @throws InterruptedException 中断异常
     */
    public void start() throws InterruptedException {
        netServer.addHandlers(Collections.singletonList(new AwareConnectHandler()));
        netServer.bind(backupNodeConfig.getBackupNodePort());
    }

    public void shutdown() {
        this.netServer.shutdown();
    }
}
