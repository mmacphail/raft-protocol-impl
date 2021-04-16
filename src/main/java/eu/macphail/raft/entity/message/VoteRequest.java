package eu.macphail.raft.entity.message;

public class VoteRequest implements NodeMessage {
    private int nodeId;
    private int term;
    private int logLength;
    private int logTerm;

    public VoteRequest() {
    }

    public VoteRequest(int nodeId, int term, int logLength, int logTerm) {
        this.nodeId = nodeId;
        this.term = term;
        this.logLength = logLength;
        this.logTerm = logTerm;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public int getLogLength() {
        return logLength;
    }

    public void setLogLength(int logLength) {
        this.logLength = logLength;
    }

    public int getLogTerm() {
        return logTerm;
    }

    public void setLogTerm(int logTerm) {
        this.logTerm = logTerm;
    }

    @Override
    public String toString() {
        return "VoteRequest{" +
                "nodeId=" + nodeId +
                ", currentTerm=" + term +
                ", size=" + logLength +
                ", lastTerm=" + logTerm +
                '}';
    }
}
