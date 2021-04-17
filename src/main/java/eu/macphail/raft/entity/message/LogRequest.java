package eu.macphail.raft.entity.message;

import eu.macphail.raft.entity.Log;

import java.util.List;

public class LogRequest implements NodeMessage {

    private int leaderId;
    private int term;
    private int logLength;
    private int logTerm;
    private int commitLength;
    private List<Log> entries;

    public LogRequest() {
    }

    public LogRequest(int leaderId, int term, int logLength, int logTerm, int commitLength, List<Log> entries) {
        this.leaderId = leaderId;
        this.term = term;
        this.logLength = logLength;
        this.logTerm = logTerm;
        this.commitLength = commitLength;
        this.entries = entries;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
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

    public int getCommitLength() {
        return commitLength;
    }

    public void setCommitLength(int commitLength) {
        this.commitLength = commitLength;
    }

    public List<Log> getEntries() {
        return entries;
    }

    public void setEntries(List<Log> entries) {
        this.entries = entries;
    }

    @Override
    public String toString() {
        return "LogRequest{" +
                "leaderId=" + leaderId +
                ", term=" + term +
                ", logLength=" + logLength +
                ", logTerm=" + logTerm +
                ", commitLength=" + commitLength +
                ", entries=" + entries +
                '}';
    }
}
