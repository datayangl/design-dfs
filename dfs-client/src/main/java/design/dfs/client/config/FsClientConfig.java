package design.dfs.client.config;

import lombok.Builder;
import lombok.Data;

/**
 * 客户端配置
 */
@Data
@Builder
public class FsClientConfig {
    private String server;
    private int port;
    private int connectRetryTime;
    private int ack = 0;
}
