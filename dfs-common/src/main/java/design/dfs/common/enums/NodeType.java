package design.dfs.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 文件节点类型
 */
@Getter
@AllArgsConstructor
public enum NodeType {
    /**
     * 1:文件
     * 2.文件夹(目录)
     */
    FILE(1),DIRECTORY(2);
    private int value;
}
