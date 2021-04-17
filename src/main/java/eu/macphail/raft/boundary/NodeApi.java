package eu.macphail.raft.boundary;

import eu.macphail.raft.component.NodeRequestQueue;
import eu.macphail.raft.entity.message.*;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class NodeApi {

    private final NodeRequestQueue nodeRequestQueue;

    public NodeApi(NodeRequestQueue nodeRequestQueue) {
        this.nodeRequestQueue = nodeRequestQueue;
    }

    @PostMapping("/vote/request")
    public void voteRequest(@RequestBody VoteRequest voteRequest) {
        nodeRequestQueue.addToQueue(voteRequest);
    }

    @PostMapping("/vote/response")
    public void voteResponse(@RequestBody VoteResponse voteResponse) {
        nodeRequestQueue.addToQueue(voteResponse);
    }

    @PostMapping("/log/request")
    public void logRequest(@RequestBody LogRequest logRequest) {
        nodeRequestQueue.addToQueue(logRequest);
    }

    @PostMapping("/log/response")
    public void logResponse(@RequestBody LogResponse logResponse) {
        nodeRequestQueue.addToQueue(logResponse);
    }

    @PostMapping("/data")
    public void postData(@RequestBody DataMessage dataMessage) {
        nodeRequestQueue.addToQueue(dataMessage);
    }
}
