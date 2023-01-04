package io.raft.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.raft.msg.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftNode {

    Logger logger = LoggerFactory.getLogger(RaftNode.class);

    private final int nodeId;

    private final List<RaftEndpoint> raftEndpoints;
    private final int majority;
    private final BlockingQueue<RaftStateMessage> raftStateMessageBlockingQueue = new LinkedBlockingQueue<>();
    private volatile NodeState nodeState = NodeState.FOLLOWER;
    private volatile ScheduledExecutorService followerTimer;

    private volatile ScheduledExecutorService leaderHeartbeatScheduledExecutorService;

    private volatile int currentTerm = 0;

    private volatile int votedFor = -1;

    private int commitIndex = 0;
    private int lastApplied = 0;
    private final Log<LogEntry> log = new Log<>();

    public RaftNode(int nodeId, List<RaftEndpoint> raftEndpoints) {
        this.nodeId = nodeId;
        this.raftEndpoints = raftEndpoints;
        this.majority = raftEndpoints.size() / 2 + 1;
        changeState(RaftStateMessage.TO_FOLLOWER);
    }

    public void initRaftStateMachine() {
        ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
        executorService.submit(() -> raftStateMachine());
    }

    void raftStateMachine() {
        while (true) {
            try {
                RaftStateMessage raftStateMessage = raftStateMessageBlockingQueue.take();
                logger.info("Take state message: " + raftStateMessage);
                switch (raftStateMessage) {
                    case TO_CANDIDATE -> {
                        nodeState = NodeState.CANDIDATE;
                        leaderElection();
                    }
                    case RESET_TIMER -> {
                        nodeState = NodeState.FOLLOWER;
                        resetFollowerTimer();
                    }
                    case TO_LEADER -> {
                        nodeState = NodeState.LEADER;
                        leaderAppendHeartbeat();
                    }
                    case TO_FOLLOWER -> {
                        nodeState = NodeState.FOLLOWER;
                        if (leaderHeartbeatScheduledExecutorService != null) {
                            leaderHeartbeatScheduledExecutorService.shutdownNow();
                        }
                        resetFollowerTimer();
                    }
                    default -> System.out.println("default");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void leaderAppendHeartbeat() {

         leaderHeartbeatScheduledExecutorService
                = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory());

        var nextIndex = new int[raftEndpoints.size()]; //Array(servers.size) {_ -> commitIndex + 1 }
        var matchIndex = new int[raftEndpoints.size()];
        Arrays.fill(nextIndex, commitIndex + 1);

        Runnable repeatedTask = () -> {
                leaderAppendHeartbeatTask(nextIndex, matchIndex);
        };

        long delay  = 1L;
        long period = 4L;
        leaderHeartbeatScheduledExecutorService.scheduleAtFixedRate(repeatedTask, delay, period, TimeUnit.SECONDS);
    }

    private void leaderAppendHeartbeatTask(int[] nextIndex, int[] matchIndex) {
        try (var executorService = Executors.newVirtualThreadPerTaskExecutor()) {
            for (var raftEndpoint : raftEndpoints) {
                executorService.submit(() -> {
                    var entries = new ArrayList<LogEntry>();
                    var i = nextIndex[raftEndpoint.id() - 1];
                    var prevLogIndex = i - 2;
                    var prevLogTerm = prevLogIndex >= 0 ? log.get(prevLogIndex).term() : -1;

                    if (log.lastIndex() >= nextIndex[raftEndpoint.id() - 1]) {
                        entries.add(log.get(i - 1));
                    }
                    var appendRequest =
                            new AppendEntriesRequest(currentTerm, nodeId, prevLogIndex, prevLogTerm, entries, commitIndex);
                    var appendResponse = appendRequest(raftEndpoint, appendRequest);
                    logger.info("endpoint: " + raftEndpoint + "; request: " + appendRequest + "; response: " + appendResponse);

                    if (appendResponse.term() > currentTerm) {
                        currentTerm = appendResponse.term();
                        logger.info("Server {0} is converted to {1} at term {2}}", nodeId, nodeState, currentTerm);
                        changeState(RaftStateMessage.TO_FOLLOWER);
                        return;
                    }

                    if (appendResponse.success()) {
                        if (entries.size() > 0) {
                            nextIndex[raftEndpoint.id() - 1] += 1;
                            matchIndex[raftEndpoint.id() - 1] += 1;

                            var count = Arrays.stream(matchIndex).filter(val -> val > commitIndex).count();
                            if (count >= majority) commitIndex += 1;
                        } else {
                            matchIndex[raftEndpoint.id() - 1] = prevLogIndex + 1;
                        }
                    } else {
                        nextIndex[raftEndpoint.id() - 1] -= 1;
                    }
                });
            }
        }
    }

    private void checkIfActualLeader(AppendEntriesResponse appendResponse) {
        if (appendResponse.term() > currentTerm) {
            currentTerm = appendResponse.term();
            changeState(RaftStateMessage.TO_FOLLOWER);
            logger.info("Server {0} is converted to {1} at term {2}}", nodeId, nodeState, currentTerm);
        }
    }

    private void resetFollowerTimer() {
        if (followerTimer != null) {
            followerTimer.shutdownNow();
        }
        startFollowerTimer();
    }

    private void startFollowerTimer() {
        followerTimer = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory());

        Runnable task = () -> {
            logger.info("The FollowerTimer elapsed");
            //System.out.println("elpased followerTimer: " + followerTimer);
            changeState(RaftStateMessage.TO_CANDIDATE);
            followerTimer.shutdownNow();
        };

        int delay = 10;
        int random = getRandomNumberInRange(delay, delay + 5);
        logger.info("Start the FollowerTimer");
        followerTimer.schedule(task, random, TimeUnit.SECONDS);
        //System.out.println("started followerTimer: " + followerTimer);
    }

    public void leaderElection() {
            try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
                executorService.submit(() -> leaderElectionTask());
            }
    }

    public void leaderElectionTask() {

            CountDownLatch countDownLatch = new CountDownLatch(majority);
            AtomicInteger votesGranted = new AtomicInteger(0);
            currentTerm++;
            votedFor = nodeId;

            logger.info("Election started: term = " + currentTerm);
            try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
                for (var raftEndpoint : raftEndpoints) {
                    executorService.submit(() -> {
                        var lastIndex = log.lastIndex();
                        var lastTerm = lastIndex == 0 ? 0 : log.get(log.lastIndex() - 1).term();
                        var voteRequest = new VoteRequest(currentTerm, nodeId, lastIndex, lastTerm);
                        VoteResponse voteResponse = requestVote(raftEndpoint, voteRequest);
                            logger.info("endpoint: {0}; request: {1}; response: {2}",
                                    raftEndpoint, voteRequest, voteResponse);
                            if (voteResponse.voteGranted()) {
                                votesGranted.incrementAndGet();
                            }
                            countDownLatch.countDown();
                    });
                }
                countDownLatch.await(10, TimeUnit.SECONDS);
                executorService.shutdownNow();
            } catch (InterruptedException e) {
                return;
            }
            logger.info("Votes granted: " + votesGranted.get());
            if (votesGranted.get() >= majority && nodeState == NodeState.CANDIDATE) {
                changeState(RaftStateMessage.TO_LEADER);
            }
            if (votesGranted.get() < majority) {
                sleep(3_000);
                if (nodeState == NodeState.CANDIDATE) {
                    //changeState(RaftState.TO_CANDIDATE);
                    leaderElectionTask();
                }
            }
    }

    public void leaderElectionTask2() {

        AtomicInteger votesGranted = new AtomicInteger(0);
        currentTerm++;
        votedFor = nodeId;

        logger.info("Election started: term = " + currentTerm);
        try (var electionScope = new FirstNTasksScope<VoteResponse>(majority)) {
            for (var raftEndpoint : raftEndpoints) {
                electionScope.fork(() -> {
                    var lastIndex = log.lastIndex();
                    var lastTerm = lastIndex == 0 ? 0 : log.get(log.lastIndex() - 1).term();
                    var voteRequest = new VoteRequest(currentTerm, nodeId, lastIndex, lastTerm);
                    VoteResponse voteResponse = requestVote(raftEndpoint, voteRequest);
                    logger.info("endpoint: {0}; request: {1}; response: {2}",
                            raftEndpoint, voteRequest, voteResponse);
                    if (voteResponse.voteGranted()) {
                        votesGranted.incrementAndGet();
                    }
                    return voteResponse;
                });
            }
            electionScope.joinUntil(Instant.now().plusSeconds(10));
        } catch (InterruptedException e) {
            return;
        }
        logger.info("Votes granted: " + votesGranted.get());
        if (votesGranted.get() >= majority && nodeState == NodeState.CANDIDATE) {
            changeState(RaftStateMessage.TO_LEADER);
        }
        if (votesGranted.get() < majority) {
            sleep(3_000);
            if (nodeState == NodeState.CANDIDATE) {
                //changeState(RaftState.TO_CANDIDATE);
                leaderElectionTask();
            }
        }
    }

    private void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private VoteResponse requestVote(RaftEndpoint raftEndpoint, VoteRequest voteRequest) {
        while(true) {
            try {
                return requestVoteTask(raftEndpoint.endpoint(), voteRequest);
            } catch (IOException e) {
                logger.warn("RaftEndpoint: " + raftEndpoint + "; VoteRequest exception: " + e);
            }
            sleep(2_000);
        }
    }

    private VoteResponse requestVoteTask(String url, VoteRequest voteRequest) throws IOException {
        try {
            return requestTask(url+"/vote", voteRequest, VoteResponse.class);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private AppendEntriesResponse appendRequest(RaftEndpoint raftEndpoint, AppendEntriesRequest appendRequest) {
        try {
            return requestTask(raftEndpoint.endpoint() + "/append", appendRequest, AppendEntriesResponse.class);
        } catch (Exception e) {
            logger.warn("RaftEndpoint: " + raftEndpoint + "; AppendRequest exception: " + e);
            throw new RuntimeException(e);
        }
    }

    private <T,V> V requestTask(String url, T requestObject, Class<V> responseType) throws IOException, InterruptedException {
        ObjectMapper objectMapper = new ObjectMapper();
        String requestBody = objectMapper
                .writerWithDefaultPrettyPrinter()
                .writeValueAsString(requestObject);

        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .timeout(Duration.ofSeconds(3))
                .build();

        String voteResponseJson = HttpClient.newHttpClient()
                .send(request, HttpResponse.BodyHandlers.ofString())
                .body();

        return objectMapper.readValue(voteResponseJson, responseType);
    }

    private int getRandomNumberInRange(int min, int max) {
        Random random = new Random();
        return random.nextInt(max - min) + min;
    }

    public void changeState(RaftStateMessage state) {
        try {
            raftStateMessageBlockingQueue.put(state);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public VoteResponse processVoteRequest(VoteRequest voteRequest) {
        var granted = false;
        if (voteRequest.term() < currentTerm) granted = false;
        else if (currentTerm == voteRequest.term()) granted = (votedFor == voteRequest.candidateId());
        else {
            if (log.lastIndex() >= 1 &&
                    voteRequest.lastLogTerm() < log.get(log.lastIndex() - 1).term())
                granted = false;
            else if (log.lastIndex() >= 1 &&
                    voteRequest.lastLogTerm() == log.get(log.lastIndex() - 1).term() &&
                    voteRequest.lastLogIndex() < log.lastIndex())
                granted = false;
            else {
                currentTerm = voteRequest.term();
                votedFor = voteRequest.candidateId();
                changeState(RaftStateMessage.TO_FOLLOWER);
                granted = true;
            }
        }
        return new VoteResponse(currentTerm, granted);
    }

    public AppendEntriesResponse processAppendRequest(AppendEntriesRequest appendRequest) {
        //resetFollowerTimer();
        if (appendRequest.term() > currentTerm) {
            currentTerm = appendRequest.term();
            votedFor = -1;
            changeState(RaftStateMessage.TO_FOLLOWER);
        }

        if (appendRequest.term() == currentTerm && appendRequest.leaderId() != nodeId) {
            changeState(RaftStateMessage.RESET_TIMER);
        }

        if (appendRequest.leaderCommit() > commitIndex) {
            commitIndex = Math.min(appendRequest.leaderCommit(), log.lastIndex());
        }

        var success = appendRequest.prevLogIndex() == -1 ||
                (log.lastIndex() > appendRequest.prevLogIndex() &&
                        log.get(appendRequest.prevLogIndex()).term() == appendRequest.prevLogTerm());

        if (success && appendRequest.entries().size() > 0)
            log.add(appendRequest.prevLogIndex() + 1, appendRequest.entries().get(0));

        return new AppendEntriesResponse(currentTerm, success);
    }

    public void appendMsg(String msg) {
        log.add(log.lastIndex(), new LogEntry(currentTerm, msg));
    }

    public List<LogEntry> log() {
        return log.entries();
    }
}
