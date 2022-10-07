package design.dfs.namenode.rebalance;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 副本复制任务
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ReplicaTask {
    private String filename;
    private String hostname;
    private int port;
}
