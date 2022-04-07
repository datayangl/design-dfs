package design.dfs.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 文件信息
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FileInfo {
    private String hostname;

    private String fileName;

    private long fileSize;


}
