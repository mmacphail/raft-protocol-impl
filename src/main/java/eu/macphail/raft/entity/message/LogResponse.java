package eu.macphail.raft.entity.message;

public class LogResponse implements NodeMessage {
    private int follower;
    private int term;
    private int ack;
    private boolean success;

    public LogResponse() {
    }

    public LogResponse(int follower, int term, int ack, boolean success) {
        this.follower = follower;
        this.term = term;
        this.ack = ack;
        this.success = success;
    }

    public int getFollower() {
        return follower;
    }

    public void setFollower(int follower) {
        this.follower = follower;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public int getAck() {
        return ack;
    }

    public void setAck(int ack) {
        this.ack = ack;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    @Override
    public String toString() {
        return "LogResponse{" +
                "follower=" + follower +
                ", term=" + term +
                ", ack=" + ack +
                ", success=" + success +
                '}';
    }
}
