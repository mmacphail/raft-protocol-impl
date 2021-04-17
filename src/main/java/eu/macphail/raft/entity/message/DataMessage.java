package eu.macphail.raft.entity.message;

public class DataMessage implements NodeMessage {
    private String content;

    public DataMessage() {
    }

    public DataMessage(String content) {
        this.content = content;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    @Override
    public String toString() {
        return "DataMessage{" +
                "content='" + content + '\'' +
                '}';
    }
}
