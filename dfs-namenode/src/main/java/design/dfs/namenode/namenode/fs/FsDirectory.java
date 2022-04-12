package design.dfs.namenode.namenode.fs;

import design.dfs.backup.fs.FsImage;
import design.dfs.common.enums.NodeType;
import design.dfs.common.utils.StringUtil;
import design.dfs.model.backup.INode;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 管理内存文件
 */
@Slf4j
public class FsDirectory {
    private Node root;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    public FsDirectory() {
        this.root = new Node("/", NodeType.DIRECTORY.getValue());
    }
    private final static char FILE_DELIMITER = '/';
    /**
     * 创建文件目录
     *
     * @param path 文件目录
     */
    public void mkdir(String path, Map<String, String> attr) {
        try {
            lock.writeLock().lock();
            String[] paths = StringUtil.split(path, FILE_DELIMITER);
            Node current = root;
            for (String p : paths) {
                if ("".equals(p)) {
                    continue;
                }
                current = findDirectory(current, p);
            }
            current.putAllAttr(attr);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 创建文件
     * @param filePath 文件全路径
     * @param attr 文件属性
     * @return
     */
    public boolean createFile(String filePath, Map<String, String> attr) {
        try {
            lock.writeLock().lock();
            String[] paths = StringUtil.split(filePath, FILE_DELIMITER);
            String fileNode = paths[paths.length - 1];
            Node parentNode = getFileParent(paths);
            Node childrenNode = parentNode.getChildren(fileNode);
            if (childrenNode != null) {
                log.warn("文件已存在，创建失败 : {}", filePath);
                return false;
            }
            // create new file node
            Node child = new Node(fileNode,NodeType.FILE.getValue());
            child.putAllAttr(attr);
            parentNode.addChildren(child);
            return true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 删除文件
     *
     * @param filename 文件名
     */
    public Node delete(String filename) {
        lock.writeLock().lock();
        try {
            String[] paths = StringUtil.split(filename, FILE_DELIMITER);
            String name = paths[paths.length - 1];
            Node current = getFileParent(paths);
            Node childrenNode;
            if ("".equals(name)) {
                childrenNode = current;
            } else {
                childrenNode = current.getChildren(name);
            }
            if (childrenNode == null) {
                log.warn("文件不存在, 删除失败：[filename={}]", filename);
                return null;
            }
            if (childrenNode.getType() == NodeType.DIRECTORY.getValue()) {
                if (!childrenNode.getChildren().isEmpty()) {
                    log.warn("文件夹存在子文件，删除失败：[filename={}]", filename);
                    return null;
                }
            }
            Node remove = current.getChildren().remove(name);

            // 删除空文件夹
            Node parent = remove.getParent();
            Node child = remove;
            while (parent != null) {
                if (child.getChildren().isEmpty()) {
                    child.setParent(null);
                    parent.getChildren().remove(child.getPath());
                }
                child = parent;
                parent = parent.getParent();
            }
            return Node.deepCopy(remove, Integer.MAX_VALUE);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 寻找文件的目录
     * 如 /a/b/c/d.txt => [a,b,c,d.txt] => c
     * @param paths
     * @return
     */
    private Node getFileParent(String[] paths) {
        Node current = root;
        for (int i = 0; i < paths.length - 1; i++) {
            String p = paths[i];
            if ("".equals(p)) {
                continue;
            }
            current = findDirectory(current, p);
        }
        return current;
    }

    private Node findDirectory(Node current, String p) {
        Node childrenNode = current.getChildren(p);
        if (childrenNode == null) {
            Node newChildrenNode = new Node(p, NodeType.DIRECTORY.getValue());
            current.addChildren(newChildrenNode);
            return newChildrenNode;
        }
        return childrenNode;
    }

    /**
     * 生成 FsImage
     */
    public FsImage createFsImage() {
        try {
            lock.readLock().lock();
            INode iNode = Node.toINode(root);
            FsImage fsImage = new FsImage(0L, iNode);
            return fsImage;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 应用 FsImage 初始化内存目录树
     * @param fsImage
     */
    public void applyFsImage(FsImage fsImage) {
        try {
            lock.writeLock().lock();
            this.root = Node.parseINode(fsImage.getINode(), "");
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 查看某个目录文件
     *
     * @param parent 目录路径
     * @return 文件路径
     */
    public Node listFiles(String parent) {
        return listFiles(parent, Integer.MAX_VALUE);
    }

    /**
     * 查看某个目录文件
     *
     * @param parent 目录路径
     * @return 文件路径
     */
    public Node listFiles(String parent, int level) {
        return Node.deepCopy(unsafeListFiles(parent), level);
    }

    /**
     * 查看某个目录文件
     *
     * @param parent 目录路径
     * @return 文件路径
     */
    public Node unsafeListFiles(String parent) {
        if (root.getPath().equals(parent)) {
            return root;
        }
        lock.readLock().lock();
        try {
            String[] paths = StringUtil.split(parent, FILE_DELIMITER);
            String name = paths[paths.length - 1];
            Node current = getFileParent(paths);
            return current.getChildren(name);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * <pre>
     *     假设存在文件：
     *
     *     /aaa/bbb/c1.png
     *     /aaa/bbb/c2.png
     *     /bbb/ccc/c3.png
     *
     * 传入：/aaa，则返回：[/bbb/c1.png, /bbb/c2.png]
     *
     * </pre>
     * <p>
     * 返回文件名
     */
    public List<String> findAllFiles(String path) {
        Node node = listFiles(path);
        if (node == null) {
            return new ArrayList<>();
        }
        return findAllFiles(node);
    }

    /**
     * 递归遍历，加入所有文件
     * @param node
     * @return
     */
    private List<String> findAllFiles(Node node) {
        List<String> ret = new ArrayList<>();
        if (node.isFile()) {
            ret.add(node.getFullPath());
        } else {
            for (String key : node.getChildren().keySet()) {
                Node child = node.getChildren().get(key);
                ret.addAll(findAllFiles(child));
            }
        }
        return ret;
    }
}
