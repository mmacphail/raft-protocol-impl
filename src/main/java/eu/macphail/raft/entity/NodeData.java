package eu.macphail.raft.entity;

import javax.persistence.*;
import java.util.Objects;

@Entity
@Table(name = "node", schema = "public", catalog = "postgres")
public class NodeData {
    private int nodeId;
    private int currentTerm;
    private Integer votedFor;
    private int commitLength;

    @Id
    @Column(name = "node_id")
    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    @Basic
    @Column(name = "current_term")
    public int getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(int currentTerm) {
        this.currentTerm = currentTerm;
    }

    @Basic
    @Column(name = "voted_for")
    public Integer getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(Integer votedFor) {
        this.votedFor = votedFor;
    }

    @Basic
    @Column(name = "commit_length")
    public int getCommitLength() {
        return commitLength;
    }

    public void setCommitLength(int commitLength) {
        this.commitLength = commitLength;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeData that = (NodeData) o;
        return nodeId == that.nodeId && currentTerm == that.currentTerm && commitLength == that.commitLength && Objects.equals(votedFor, that.votedFor);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, currentTerm, votedFor, commitLength);
    }
}
