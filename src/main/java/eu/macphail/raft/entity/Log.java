package eu.macphail.raft.entity;

import javax.persistence.*;
import java.util.Arrays;
import java.util.Objects;

@Entity
@Table(name = "node_log", schema = "public", catalog = "postgres")
@IdClass(LogPK.class)
public class Log {
    private int nodeId;
    private int logOffset;
    private int term;
    private byte[] data;

    @Id
    @Column(name = "node_id")
    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    @Id
    @Column(name = "log_offset")
    public int getLogOffset() {
        return logOffset;
    }

    public void setLogOffset(int logOffset) {
        this.logOffset = logOffset;
    }

    @Basic
    @Column(name = "term")
    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    @Basic
    @Column(name = "data")
    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Log that = (Log) o;
        return nodeId == that.nodeId && logOffset == that.logOffset && Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(nodeId, logOffset);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public String toString() {
        return "Log{" +
                "nodeId=" + nodeId +
                ", logOffset=" + logOffset +
                ", term=" + term +
                '}';
    }
}
