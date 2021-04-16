package eu.macphail.raft.entity.message;

public class VoteResponse implements NodeMessage {

    int nodeId;
    int term;
    boolean granted;

    public VoteResponse() {
    }

    public VoteResponse(int nodeId, int term, boolean granted) {
        this.nodeId = nodeId;
        this.term = term;
        this.granted = granted;
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

    public boolean isGranted() {
        return granted;
    }

    public void setGranted(boolean granted) {
        this.granted = granted;
    }
}
