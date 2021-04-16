package eu.macphail.raft.entity;

import javax.persistence.Column;
import javax.persistence.Id;
import java.io.Serializable;
import java.util.Objects;

public class LogPK implements Serializable {
    private int nodeId;
    private int logOffset;

    @Column(name = "node_id")
    @Id
    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    @Column(name = "log_offset")
    @Id
    public int getLogOffset() {
        return logOffset;
    }

    public void setLogOffset(int logOffset) {
        this.logOffset = logOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogPK that = (LogPK) o;
        return nodeId == that.nodeId && logOffset == that.logOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, logOffset);
    }
}
