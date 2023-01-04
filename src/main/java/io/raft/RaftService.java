package io.raft;

import io.raft.core.RaftEndpoint;
import io.raft.core.RaftNode;
import io.raft.msg.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;

@Service
public class RaftService {

    @Value("${raft.nodeId}")
    private Integer nodeId;

    @Value("#{${raft.raftEndpoints}}")
    private Map<Integer, String> raftNodes;

    private List<RaftEndpoint> raftEndpoints = List.of();

    private RaftNode raftNode;

    @PostConstruct
    private void init() {
        raftEndpoints = raftNodes.entrySet().stream()
                .map(entry -> new RaftEndpoint(entry.getKey(), "http://localhost:" + entry.getValue()))
                .toList();
        raftNode = new RaftNode(nodeId, raftEndpoints);
        raftNode.initRaftStateMachine();
    }

    public void resetTimer() {
        raftNode.changeState(RaftStateMessage.RESET_TIMER);
    }

    public VoteResponse processVoteRequest(VoteRequest voteRequest) {
        return raftNode.processVoteRequest(voteRequest);
    }

    public AppendEntriesResponse processAppendRequest(AppendEntriesRequest appendRequest) {
        return raftNode.processAppendRequest(appendRequest);
    }

    public List<LogEntry> getLog() {
        return raftNode.log();
    }

    public List<LogEntry> appendMsg(String msg) {
        raftNode.appendMsg(msg);
        return getLog();
    }
}
