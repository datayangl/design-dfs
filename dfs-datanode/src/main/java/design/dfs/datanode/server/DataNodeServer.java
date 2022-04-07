package design.dfs.datanode.server;

import design.dfs.common.network.NetServer;
import design.dfs.common.utils.DefaultScheduler;
import design.dfs.datanode.config.DataNodeConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;


@Slf4j
public class DataNodeServer {
    private DataNodeApis dataNodeApis;
    private NetServer netServer;
    private DataNodeConfig dataNodeConfig;

    public DataNodeServer(DataNodeConfig dataNodeConfig, DefaultScheduler defaultScheduler, DataNodeApis dataNodeApis) {
        this.dataNodeConfig = dataNodeConfig;
        this.dataNodeApis = dataNodeApis;
        this.netServer = new NetServer("DataNode-Server", defaultScheduler, dataNodeConfig.getDataNodeWorkerThreads());
    }

    public void start() throws InterruptedException {
        this.netServer.bind(Arrays.asList(dataNodeConfig.getDataNodeTransportPort(), dataNodeConfig.getDataNodeHttpPort()));
    }

    /**
     * 优雅停止
     */
    public void shutdown() {
        log.info("Shutdown DataNodeServer");
        this.netServer.shutdown();
    }
}
