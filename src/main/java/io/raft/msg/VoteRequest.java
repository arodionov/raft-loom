package io.raft.msg;

public record VoteRequest(Integer term, Integer candidateId, Integer lastLogIndex, Integer lastLogTerm) {}
