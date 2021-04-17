package eu.macphail.raft.component;

import eu.macphail.raft.entity.message.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class MessageDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(MessageDispatcher.class);

    @Value("${nodes}")
    private List<Integer> nodes;

    @Value("${nodes.url}")
    private List<String> nodeUrls;

    private Map<Integer, String> urls;

    private RestTemplate restTemplate;

    @PostConstruct
    public void init() {
        urls = new HashMap<>();

        for(int i=0; i<nodes.size(); i++) {
            urls.put(nodes.get(i), nodeUrls.get(i));
        }

        restTemplate = new RestTemplate();
    }

    public void dispatch(int nodeId, NodeMessage message) {
        LOG.info("Dispatching message {} to node {}", message, nodeId);

        String url = "http://" + urls.get(nodeId);
        if(message instanceof LogRequest) {
            url += "/log/request";
        } else if(message instanceof LogResponse) {
            url += "/log/response";
        } else if(message instanceof VoteRequest) {
            url += "/vote/request";
        } else if(message instanceof VoteResponse) {
            url += "/vote/response";
        }

        try {
            ResponseEntity<String> response = restTemplate.postForEntity(url, message, String.class);
            if (!response.getStatusCode().is2xxSuccessful()) {
                LOG.error("Unable to send message to node {}. Status code: {}, body:\n{}",
                        nodeId, response.getStatusCodeValue(), response.getBody());
            }
        } catch(RestClientException e) {
            LOG.error("Unable to send message to node {}. Exception: {}",
                    nodeId, e.getMessage());
        }
    }

    public void forwardToLeader(Integer leader, NodeMessage nodeMessage) {
        LOG.info("Forwarding message {} to leader {}", nodeMessage, leader);
        dispatch(leader, nodeMessage);
    }

    public void deliver(byte[] data) {
        LOG.info("Delivering data");
    }
}
