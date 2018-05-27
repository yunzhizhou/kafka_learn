package com.github.zyz.kafka.zk;

/**
 * @author zyz
 * @since 2018/4/24
 */
public class NodeInfo {

    private final Integer id;
    private final String nodePath;

    public NodeInfo(Integer id, String nodePath) {
        this.id = id;
        this.nodePath = nodePath;
    }

    public Integer getId() {
        return id;
    }

    public String getNodePath() {
        return nodePath;
    }

    @Override
    public String toString() {
        return "NodeInfo{" +
                "id=" + id +
                ", nodePath='" + nodePath + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NodeInfo nodeInfo = (NodeInfo) o;

        if (id != null ? !id.equals(nodeInfo.id) : nodeInfo.id != null) return false;
        return nodePath != null ? nodePath.equals(nodeInfo.nodePath) : nodeInfo.nodePath == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (nodePath != null ? nodePath.hashCode() : 0);
        return result;
    }
}