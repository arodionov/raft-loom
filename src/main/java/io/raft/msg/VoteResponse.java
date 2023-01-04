package io.raft.msg;

public record VoteResponse(Integer term, Boolean voteGranted) { }
