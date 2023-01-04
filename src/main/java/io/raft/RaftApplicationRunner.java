package io.raft;

import io.raft.core.RaftEndpoint;
import io.raft.core.RaftNode;

import java.io.IOException;
import java.util.List;

public class RaftApplicationRunner {
    public static void main(String[] args) throws IOException, InterruptedException {

        var raftEndpoints = List.of(
                new RaftEndpoint(1, "http://localhost:8080"),
                new RaftEndpoint(2, "http://localhost:8081"),
                new RaftEndpoint(3, "http://localhost:8082")
        );
        RaftNode raftNode = new RaftNode(1, raftEndpoints);
        raftNode.initRaftStateMachine();
    }
}
