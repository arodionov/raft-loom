package io.raft;

import io.raft.msg.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class RaftController {

    private Logger logger = LoggerFactory.getLogger(RaftController.class);

    private final RaftService raftService;

    public RaftController(RaftService raftService) {
        this.raftService = raftService;
    }

    @GetMapping("/log")
    public List<LogEntry> getLog() {
        return raftService.getLog();
    }

    @GetMapping("/add/{msg}")
    public List<LogEntry> appendMessage(@PathVariable String msg) {
        return raftService.appendMsg(msg);
    }

    @PostMapping("/vote")
    public VoteResponse voteRequest(@RequestBody VoteRequest voteRequest) throws InterruptedException {
        logger.info(voteRequest.toString());
        return raftService.processVoteRequest(voteRequest);
        //return new VoteResponse(1, true);
    }

    @PostMapping("/append")
    public AppendEntriesResponse appendRequest(@RequestBody AppendEntriesRequest appendRequest) throws InterruptedException {
        logger.info(appendRequest.toString());
//        raftService.resetTimer();
//        return new AppendEntriesResponse(1, true);

        return raftService.processAppendRequest(appendRequest);
    }
}
