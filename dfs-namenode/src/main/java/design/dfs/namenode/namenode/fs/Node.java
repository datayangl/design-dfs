package design.dfs.namenode.namenode.fs;

import design.dfs.common.enums.NodeType;
import design.dfs.model.backup.INode;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.*;

/**
 * 文件系统节点，文件or目录，包含子节点
 *
 */
@Data
@Slf4j
public class Node {
    private String path;
    private int type;
    private final TreeMap<String, Node> children;
    private Map<String, String> attr;
    private Node parent;

    public Node() {
        this.children = new TreeMap<>();
        this.attr = new HashMap<>();
        this.parent = null;
    }

    public Node(String path, int type) {
        this();
        this.path = path;
        this.type = type;
    }

    /**
     * 是否是一个文件
     *
     * @return 是否是一个文件
     */
    public boolean isFile() {
        return type == NodeType.FILE.getValue();
    }

    /**
     * 获取当前节点的全名路径
     *
     * @return 当前节点的全路径
     */
    public String getFullPath() {
        return getFullPathInternal(this);
    }

    private String getFullPathInternal(Node parent) {
        if (parent == null) {
            return null;
        }
        String parentPath = getFullPathInternal(parent.getParent());
        if (parentPath == null) {
            return "";
        }
        return parentPath + "/" + parent.path;
    }

    /**
     * Node -> INode
     * INode 可以理解为 Node 的 ProtoBuf 版本，便于数据的序列化和构造，使用递归 DFS 算法进行转换
     * @param node
     * @return
     */
    public static INode toINode(Node node) {
        INode.Builder builder = INode.newBuilder();
        String path = node.getPath();
        int type = node.type;
        builder.setPath(path);
        builder.setType(type);
        builder.putAllAttr(node.getAttr());
        Collection<Node> children = node.getChildren().values();
        if (children.isEmpty()) {
            return  builder.build();
        }
        List<INode> tmpNodes = new ArrayList<>(children.size());
        for (Node child : children) {
            INode iNode = toINode(child);
            tmpNodes.add(iNode);
        }
        builder.addAllChildren(tmpNodes);
        return builder.build();
    }

    public static Node parseINode(INode iNode) {
        return parseINode(iNode, null);
    }

    public static Node parseINode(INode iNode, String parent) {
        Node node = new Node();
        if (parent != null && log.isDebugEnabled()) {
            log.debug("parseINode executing :[path={},  type={}]", parent, node.getType());
        }
        String path = iNode.getPath();
        int type = iNode.getType();
        node.setPath(path);
        node.setType(type);
        node.putAllAttr(iNode.getAttrMap());
        List<INode> children = iNode.getChildrenList();
        if (children.isEmpty()) {
            return node;
        }

        for (INode child : children) {
            node.addChildren(parseINode(child, parent == null? null : parent + File.separator + child.getPath()));
        }
        return node;
    }

    /**
     * 递归 深度拷贝节点
     *
     * @param node  节点
     * @param level 拷贝多少个孩子层级
     * @return 拷贝节点
     */
    public static Node deepCopy(Node node, int level) {
        if (node == null) {
            return null;
        }
        Node ret = new Node();
        String path = node.getPath();
        int type = node.getType();
        ret.setPath(path);
        ret.setType(type);
        ret.putAllAttr(node.getAttr());
        if (level > 0) {
            TreeMap<String, Node> children = node.children;
            if (!children.isEmpty()) {
                for (String key : children.keySet()) {
                    ret.addChildren(deepCopy(children.get(key), level - 1));
                }
            }
        }
        return ret;
    }

    /**
     * 添加一个孩子节点
     *
     * @param child 孩子节点
     */
    public void addChildren(Node child) {
        synchronized (children) {
            child.setParent(this);
            this.children.put(child.getPath(), child);
        }
    }

    /**
     * 通过 文件名or目录名 获取孩子节点
     *
     * @param child 孩子节点
     */
    public Node getChildren(String child) {
        synchronized (children) {
            return children.get(child);
        }
    }

    public void putAllAttr(Map<String, String> attr) {
        this.attr.putAll(attr);
    }

    @Override
    public String toString() {
        return "Node{" +
                "path='" + path + '\'' +
                ", type=" + type +
                ", children=" + children +
                ", attr=" + attr +
                '}';
    }
}
