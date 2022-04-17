package design.dfs.client.fs;

import design.dfs.common.Constants;
import design.dfs.model.backup.INode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * 文件属性
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FsFile {
    private int type;
    private String path;
    private String fileSize;

    public List<FsFile> parse(INode iNode) {
        List<FsFile> fsFiles = new ArrayList<>();
        if (iNode == null) {
            return fsFiles;
        }

        List<INode> childrenList = iNode.getChildrenList();
        if (childrenList.isEmpty()) {
            return fsFiles;
        }
        for (INode child : childrenList) {
            FsFile file = new FsFile();
            file.setPath(child.getPath());
            file.setType(child.getType());
            long fileSize = Long.parseLong(iNode.getAttrMap().getOrDefault(Constants.ATTR_FILE_SIZE, "0"));
            file.setFileSize(String.valueOf(fileSize));
            fsFiles.add(file);
        }

        return fsFiles;
    }
}
