package eu.macphail.raft.boundary;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.macphail.raft.component.LogRepository;
import eu.macphail.raft.component.MessageDispatcher;
import eu.macphail.raft.entity.Log;
import eu.macphail.raft.entity.message.*;
import eu.macphail.raft.entity.Role;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Component
public class Node {

    @Value("${nodes}")
    private List<Integer> nodes;

    @Value("${node.id}")
    private int nodeId;

    @Value("${raft.election-timer.timeout.ms}")
    private int electionTimerTimeoutMs;

    @Value("${raft.leader.timeout.ms}")
    private int leaderTimeoutMs;

    @Value("${raft.replicate.log.interval.ms}")
    private int replicateLogIntervalMs;

    private int currentTerm;
    private Integer votedFor;
    private List<Log> log;
    private int commitLength;

    private Role currentRole;
    private Integer currentLeader;
    private List<Integer> votesReceived;
    private Map<Integer, Integer> sentLength;
    private Map<Integer, Integer> ackedLength;

    private Timer electionTimer;
    private Timer leaderTimer;
    private Timer replicateLogTimer;

    private static final Logger LOG = LoggerFactory.getLogger(Node.class);

    private final MessageDispatcher dispatcher;
    private final LogRepository logRepository;
    private final ObjectMapper objectMapper;

    public Node(MessageDispatcher dispatcher, LogRepository logRepository, ObjectMapper objectMapper) {
        this.dispatcher = dispatcher;
        this.logRepository = logRepository;
        this.objectMapper = objectMapper;
    }

    public synchronized void init(int currentTerm, Integer votedFor, List<Log> log, int commitLength) {
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        loadLog();
        this.commitLength = commitLength;
        this.currentRole = Role.FOLLOWER;
        this.currentLeader = null;
        this.votesReceived = new ArrayList<>();
        this.sentLength = initNodesMap();
        this.ackedLength = initNodesMap();

        LOG.debug("Node initialized {}", this);
        LOG.debug("Known nodes: {}", nodes);

        launchLeaderTimeoutTimer();
    }

    private Map<Integer, Integer> initNodesMap() {
        Map<Integer, Integer> m = new HashMap<>();
        for (Integer node : nodes) {
            m.put(node, 0);
        }
        return m;
    }

    public synchronized void onLeaderFailureOrElectionTimeout() {
        currentTerm += 1;
        currentRole = Role.CANDIDATE;
        votedFor = nodeId;
        votesReceived = new ArrayList<>();
        votesReceived.add(nodeId);
        int lastTerm = 0;
        if (!log.isEmpty()) {
            lastTerm = log.get(log.size() - 1).getTerm();
        }
        VoteRequest voteRequest = new VoteRequest(nodeId, currentTerm, log.size(), lastTerm);
        nodes.forEach(node -> dispatcher.dispatch(node, voteRequest));

        startElectionTimer();
    }

    public synchronized void onVoteRequest(VoteRequest voteRequest) {
        int cId = voteRequest.getNodeId();
        int cLogTerm = voteRequest.getLogTerm();
        int cLogLength = voteRequest.getLogLength();
        int myLogTerm = log.get(log.size() - 1).getTerm();
        boolean logOk = cLogTerm > myLogTerm ||
                (cLogTerm == myLogTerm && cLogLength >= log.size());
        int term = voteRequest.getTerm();
        boolean termOk = term > currentTerm ||
                (term == currentTerm && (votedFor == null || votedFor == cId));

        if (logOk && termOk) {
            currentTerm = term;
            currentRole = Role.FOLLOWER;
            votedFor = cId;
            dispatcher.dispatch(cId, new VoteResponse(this.nodeId, currentTerm, true));
        } else {
            dispatcher.dispatch(cId, new VoteResponse(this.nodeId, currentTerm, false));
        }
    }

    public synchronized void onVoteResponse(VoteResponse voteResponse) {
        int voterId = voteResponse.getNodeId();
        int term = voteResponse.getTerm();
        boolean granted = voteResponse.isGranted();
        if (currentRole == Role.CANDIDATE && term == currentTerm && granted) {
            votesReceived.add(voterId);
            if (votesReceived.size() >= nodeQuorum()) {
                currentRole = Role.LEADER;
                currentLeader = this.nodeId;
                cancelElectionTimer();
                followers().forEach(follower -> {
                    sentLength.put(follower, log.size());
                    ackedLength.put(follower, 0);
                    replicateLog(this.nodeId, follower);
                });
            }
        } else if (term > currentTerm) {
            currentTerm = term;
            currentRole = Role.FOLLOWER;
            votedFor = null;
            cancelElectionTimer();
        }
    }

    public synchronized void onLogRequest(LogRequest logRequest) {
        int leaderId = logRequest.getLeaderId();
        int term = logRequest.getTerm();
        int logLength = logRequest.getLogLength();
        int leaderCommit = logRequest.getLeaderCommit();
        List<Log> entries = logRequest.getEntries();

        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
            currentRole = Role.FOLLOWER;
            currentLeader = leaderId;
            launchLeaderTimeoutTimer();
        }
        if (term == currentTerm && currentRole == Role.CANDIDATE) {
            currentRole = Role.FOLLOWER;
            currentLeader = leaderId;
            launchLeaderTimeoutTimer();
        }
        boolean logOk = log.size() >= logLength
                && (logLength == 0
                || term == log.get(logLength - 1).getTerm());
        if (term == currentTerm && logOk) {
            appendEntries(logLength, leaderCommit, entries);
            int ack = logLength + entries.size();
            dispatcher.dispatch(leaderId, new LogResponse(nodeId, currentTerm, ack, true));
        } else {
            dispatcher.dispatch(leaderId, new LogResponse(nodeId, currentTerm, 0, false));
        }
    }

    public synchronized void onLogResponse(LogResponse response) {
        int follower = response.getFollower();
        int term = response.getTerm();
        int ack = response.getAck();
        boolean success = response.isSuccess();

        if (term == currentTerm && currentRole == Role.LEADER) {
            if (success && ack >= ackedLength.get(follower)) {
                sentLength.put(follower, ack);
                ackedLength.put(follower, ack);
                commitLogEntries();
            } else if (sentLength.get(follower) > 0) {
                sentLength.put(follower, sentLength.get(follower) - 1);
                replicateLog(nodeId, follower);
            }
        } else if (term > currentTerm) {
            currentTerm = term;
            currentRole = Role.FOLLOWER;
            votedFor = null;
        }
    }

    private void commitLogEntries() {
        int minAcks = nodeQuorum();
        List<Integer> ready = IntStream.rangeClosed(1, log.size())
                .filter(l -> acks(l) >= minAcks)
                .boxed().collect(Collectors.toList());
        Optional<Integer> max = ready.stream().max(Integer::compareTo);
        if(!ready.isEmpty() && max.get() > commitLength
            && log.get((max.get() - 1)).getTerm() == currentTerm) {
            for(int i=commitLength; i<max.get(); i++) {
                dispatcher.deliver(log.get(i).getData());
            }
            commitLength = max.get();
        }
    }

    private int acks(int length) {
        return (int) nodes.stream().filter(n -> ackedLength.get(n) >= length).count();
    }

    private void appendEntries(int logLength, int leaderCommit, List<Log> entries) {
        if (entries.size() > 0 && log.size() > entries.size()) {
            if (log.get(logLength).getTerm() != entries.get(0).getTerm()) {
                truncateLog(logLength - 1);
            }
        }
        if (logLength + entries.size() > log.size()) {
            for (int i = log.size() - entries.size(); i < entries.size(); i++) {
                appendEntryToLog(entries.get(i));
            }
        }
        if (leaderCommit > commitLength) {
            for (int i = commitLength; i < leaderCommit; i++) {
                dispatcher.deliver(entries.get(i).getData());
            }
            commitLength = leaderCommit;
        }
    }

    private int nodeQuorum() {
        return (int) Math.ceil(((double) (nodes.size() + 1)) / 2);
    }

    public synchronized void broadcastMessage(NodeMessage nodeMessage) {
        if (currentRole == Role.LEADER) {
            saveToLog(nodeMessage);
            ackedLength.put(nodeId, log.size());

            followers().forEach(follower -> replicateLog(nodeId, follower));

            LOG.debug("Sent message {}", nodeMessage);
            LOG.debug("New log: {}", log);
        } else {
            dispatcher.forwardToLeader(currentLeader, nodeMessage);
        }
    }

    public synchronized void sendReplicateLog() {
        if (currentRole == Role.LEADER) {
            followers().forEach(follower -> replicateLog(nodeId, follower));
        }
        launchReplicateLogTimer();
    }

    private void saveToLog(NodeMessage nodeMessage) {
        try {
            Log l = new Log();
            l.setNodeId(nodeId);
            l.setLogOffset(l.getLogOffset() + 1);
            l.setTerm(currentTerm);
            objectMapper.writeValueAsString(nodeMessage);
            logRepository.save(l);
            loadLog();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private void loadLog() {
        this.log = logRepository.findByNodeIdOrderByLogOffset(nodeId);
    }

    private void truncateLog(int index) {
        logRepository.truncateLog(nodeId, index);
        loadLog();
    }

    private void appendEntryToLog(Log l) {
        int newOffset = log.size();
        Log l2 = new Log();
        l2.setNodeId(nodeId);
        l2.setTerm(currentTerm);
        l2.setLogOffset(newOffset);
        l2.setData(l.getData());
        logRepository.save(l2);
        loadLog();
    }

    private void replicateLog(int leaderId, int followerId) {
        int i = sentLength.get(followerId);
        List<Log> entries = getEntriesFromLog(i);
        int prevLogTerm = 0;
        if (i > 0) {
            prevLogTerm = log.get(i - 1).getTerm();
        }
        dispatcher.dispatch(followerId, new LogRequest(leaderId, currentTerm, i, prevLogTerm, commitLength, entries));
    }

    private List<Log> getEntriesFromLog(int index) {
        List<Log> l = new ArrayList<>();
        for (int i = index; i < log.size() - 1; i++) {
            l.add(log.get(i));
        }
        return l;
    }

    private List<Integer> followers() {
        return nodes.stream()
                .filter(i -> i != nodeId)
                .collect(Collectors.toList());
    }

    private synchronized void startElectionTimer() {
        if(electionTimer != null) {
            electionTimer.cancel();
            electionTimer = null;
        } else {
            electionTimer = new Timer(true);
            electionTimer.schedule(new TimerTask() {
                @Override
                public void run() {
                    onLeaderFailureOrElectionTimeout();
                }
            }, electionTimerTimeoutMs);
        }
    }

    private synchronized void cancelElectionTimer() {
        if(electionTimer != null) {
            electionTimer.cancel();
        }
    }

    private synchronized void launchLeaderTimeoutTimer() {
        if(leaderTimer != null) {
            leaderTimer.cancel();
        }
        leaderTimer = new Timer(true);
        leaderTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                onLeaderFailureOrElectionTimeout();
            }
        }, leaderTimeoutMs);
    }

    private synchronized void launchReplicateLogTimer() {
        if(replicateLogTimer != null) {
            replicateLogTimer.cancel();
        }
        replicateLogTimer = new Timer(true);
        replicateLogTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                sendReplicateLog();
            }
        }, replicateLogIntervalMs);
    }

    @Override
    public String toString() {
        return "Node{" +
                "nodeId=" + nodeId +
                ", currentTerm=" + currentTerm +
                ", votedFor=" + votedFor +
                ", log=" + log +
                ", commitLength=" + commitLength +
                ", currentRole=" + currentRole +
                ", currentLeader=" + currentLeader +
                ", votesReceived=" + votesReceived +
                ", sentLength=" + sentLength +
                ", ackedLength=" + ackedLength +
                '}';
    }
}
