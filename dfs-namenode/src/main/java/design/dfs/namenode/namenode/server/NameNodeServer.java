package design.dfs.namenode.namenode.server;

import design.dfs.common.network.NetServer;
import design.dfs.common.utils.DefaultScheduler;
import design.dfs.namenode.namenode.fs.DiskFileSystem;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;

@Slf4j
public class NameNodeServer {
    private NameNodeApis nameNodeApis;
    private DiskFileSystem diskNameSystem;
    private NetServer netServer;

    public NameNodeServer(DefaultScheduler defaultScheduler, DiskFileSystem diskFileSystem, NameNodeApis nameNodeApis) {
        this.diskNameSystem = diskFileSystem;
        this.nameNodeApis = nameNodeApis;
        this.netServer = new NetServer("NameNode-Server", defaultScheduler);
    }
    /**
     * 启动一个Socket Server，监听指定的端口号
     */
    public void start() throws InterruptedException {
        this.netServer.addHandlers(Collections.singletonList(nameNodeApis));
        netServer.bind(diskNameSystem.getNameNodeConfig().getPort());
    }

    /**
     * 停止服务
     */
    public void shutdown() {
        log.info("Shutdown NameNodeServer.");
        netServer.shutdown();
    }
}
