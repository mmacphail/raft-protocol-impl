package eu.macphail.raft.boundary;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.macphail.raft.component.LogRepository;
import eu.macphail.raft.component.MessageDispatcher;
import eu.macphail.raft.entity.Log;
import eu.macphail.raft.entity.Role;
import eu.macphail.raft.entity.message.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
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

    // TODO persist variables on storage
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
        this.log = log;
        this.commitLength = commitLength;
        this.currentRole = Role.FOLLOWER;
        this.currentLeader = null;
        this.votesReceived = new ArrayList<>();
        this.sentLength = initNodesMap();
        this.ackedLength = initNodesMap();

        LOG.debug("Node initialized {}", this);
        LOG.debug("Known nodes: {}", nodes);

        Random rng = new Random();
        electionTimerTimeoutMs += rng.nextInt(1500);
        launchElectionTimer();
        launchReplicateLogTimer();
    }

    private Map<Integer, Integer> initNodesMap() {
        Map<Integer, Integer> m = new HashMap<>();
        for (Integer node : nodes) {
            m.put(node, 0);
        }
        return m;
    }

    public synchronized void onLeaderFailureOrElectionTimeout() {
        LOG.debug("Leader failure or election timeout, voting for self and dispatch a vote request");
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

        launchElectionTimer();
    }

    public synchronized void onVoteRequest(VoteRequest voteRequest) {
        int cId = voteRequest.getNodeId();
        int cTerm = voteRequest.getTerm();
        int cLogTerm = voteRequest.getLogTerm();
        int cLogLength = voteRequest.getLogLength();
        LOG.debug("Received vote request from node {} and term {}", cId, cTerm);
        boolean notRequestFromMyself = !(currentRole == Role.CANDIDATE && cId == nodeId);
        int myLogTerm = log.isEmpty() ? 0 : log.get(log.size() - 1).getTerm();
        LOG.debug("myLogTerm: {}", myLogTerm);
        boolean logOk = cLogTerm > myLogTerm ||
                (cLogTerm == myLogTerm && cLogLength >= log.size());
        LOG.debug("cLogTerm: {}", cLogTerm);
        LOG.debug("cLogLength: {}", cLogLength);
        LOG.debug("logOK: {}", logOk);
        boolean termOk = cTerm > currentTerm ||
                (cTerm == currentTerm && (votedFor == null || votedFor == cId));
        LOG.debug("termOK: {}", termOk);
        LOG.debug("votedFor: {}", votedFor);
        if (notRequestFromMyself) {
            if (logOk && termOk) {
                currentTerm = cTerm;
                currentRole = Role.FOLLOWER;
                votedFor = cId;
                LOG.debug("node {} will vote for {}", nodeId, cId);
                dispatcher.dispatch(cId, new VoteResponse(this.nodeId, currentTerm, true));
            } else {
                LOG.debug("node {} will not vote for {}", nodeId, cId);
                dispatcher.dispatch(cId, new VoteResponse(this.nodeId, currentTerm, false));
            }
        }
    }

    public synchronized void onVoteResponse(VoteResponse voteResponse) {
        int voterId = voteResponse.getNodeId();
        int term = voteResponse.getTerm();
        boolean granted = voteResponse.isGranted();
        LOG.debug("Received vote response from {}. Response is {}.", voterId, granted);
        LOG.debug("Current role: {}, term: {}, current term: {}.", currentRole, term, currentTerm);
        if (currentRole == Role.CANDIDATE && term == currentTerm && granted) {
            votesReceived.add(voterId);
            LOG.debug("Votes received: {}", votesReceived);
            if (votesReceived.size() >= nodeQuorum()) {
                LOG.debug("Node {} has been elected as the leader", nodeId);
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
        int leaderCommit = logRequest.getCommitLength();
        List<Log> entries = logRequest.getEntries();

        LOG.debug("Received log request from {} at term {} (self term is {})", leaderId, term, currentTerm);

        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
            currentRole = Role.FOLLOWER;
            currentLeader = leaderId;
            LOG.debug("Accepting {} as leader, updating current term to {}, setting myself as follower", leaderId, currentTerm);
        }
        if (term == currentTerm && currentRole == Role.CANDIDATE) {
            currentRole = Role.FOLLOWER;
            currentLeader = leaderId;
            LOG.debug("Accepting {} as leader, setting myself as follower", leaderId);
        }
        boolean logOk = log.size() >= logLength
                && (logLength == 0
                || term == log.get(logLength - 1).getTerm());
        if (term == currentTerm && logOk) {
            // We assume this node is the leader
            // This is a modification from Martin's algorithm
            currentLeader = leaderId;
            LOG.debug("Accepting node {} as leader", currentLeader);
            appendEntries(logLength, leaderCommit, entries);
            int ack = logLength + entries.size();
            dispatcher.dispatch(leaderId, new LogResponse(nodeId, currentTerm, ack, true));
        } else {
            dispatcher.dispatch(leaderId, new LogResponse(nodeId, currentTerm, 0, false));
        }
        launchLeaderTimeoutTimer();
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
        if (!ready.isEmpty() && max.get() > commitLength
                && log.get((max.get() - 1)).getTerm() == currentTerm) {
            for (int i = commitLength; i < max.get(); i++) {
                dispatcher.deliver(log.get(i).getData());
            }
            commitLength = max.get();
        }
    }

    private int acks(int length) {
        return (int) nodes.stream().filter(n -> ackedLength.get(n) >= length).count();
    }

    private void appendEntries(int logLength, int leaderCommit, List<Log> entries) {
        LOG.debug("Appending entries logLength: {}, leaderCommit: {}, entries: {}", logLength, leaderCommit, entries);
        LOG.debug("Current log is: {}", log);
        LOG.debug("Current commit length is: {}", commitLength);
        if (entries.size() > 0 && log.size() > logLength) {
            if (log.get(logLength).getTerm() != entries.get(0).getTerm()) {
                LOG.debug("Truncating log to {}", logLength - 1);
                truncateLog(logLength - 1);
            }
        }
        if (logLength + entries.size() > log.size()) {
            LOG.debug("Appending entries from index {} to log", log.size() - logLength);
            for (int i = log.size() - logLength; i < entries.size(); i++) {
                appendEntryToLog(entries.get(i));
            }
        }
        if (leaderCommit > commitLength) {
            for (int i = commitLength; i < leaderCommit; i++) {
                dispatcher.deliver(log.get(i).getData());
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
            LOG.debug("New log: {}", log);
            ackedLength.put(nodeId, log.size());

            followers().forEach(follower -> replicateLog(nodeId, follower));
            LOG.debug("Replicated new log");
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
            l.setLogOffset(log.size());
            l.setTerm(currentTerm);
            String data = objectMapper.writeValueAsString(nodeMessage);
            l.setData(data.getBytes(StandardCharsets.UTF_8));
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
        LOG.debug("Replicating log on {} (leader: {}) to follower: {}", nodeId, leaderId, followerId);
        int i = sentLength.get(followerId);
        LOG.debug("Log sent to follower: {}", i);
        List<Log> entries = getEntriesFromLog(i);
        LOG.debug("Entries from {}: {}", i, entries);
        int prevLogTerm = 0;
        if (i > 0) {
            prevLogTerm = log.get(i - 1).getTerm();
        }
        LOG.debug("Previous log term: {}", prevLogTerm);
        LOG.debug("Dispatching log request to follower: {}", followerId);
        dispatcher.dispatch(followerId, new LogRequest(leaderId, currentTerm, i, prevLogTerm, commitLength, entries));
    }

    private List<Log> getEntriesFromLog(int index) {
        LOG.debug("Get entries from log index: {}, log: {}", index, log);
        List<Log> l = new ArrayList<>();
        for (int i = index; i < log.size(); i++) {
            l.add(log.get(i));
        }
        return l;
    }

    private List<Integer> followers() {
        return nodes.stream()
                .filter(i -> i != nodeId)
                .collect(Collectors.toList());
    }

    private synchronized void launchElectionTimer() {
        if (electionTimer != null) {
            electionTimer.cancel();
        }
        electionTimer = new Timer(true);
        electionTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                LOG.info("Election timeout");
                onLeaderFailureOrElectionTimeout();
            }
        }, electionTimerTimeoutMs);
        LOG.info("Election timeout timer started");
    }

    private synchronized void cancelElectionTimer() {
        if (electionTimer != null) {
            electionTimer.cancel();
        }
        LOG.info("Election timeout timer canceled");
    }

    private synchronized void launchLeaderTimeoutTimer() {
        if (leaderTimer != null) {
            leaderTimer.cancel();
        }
        leaderTimer = new Timer(true);
        leaderTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                LOG.info("Leader timeout");
                onLeaderFailureOrElectionTimeout();
            }
        }, leaderTimeoutMs);
        LOG.info("Leader timeout timer started");
    }

    private synchronized void launchReplicateLogTimer() {
        if (replicateLogTimer != null) {
            replicateLogTimer.cancel();
        }
        replicateLogTimer = new Timer(true);
        replicateLogTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                LOG.info("Replicate log timer ended");
                sendReplicateLog();
            }
        }, replicateLogIntervalMs);
        LOG.info("Replicate log timer started");
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
