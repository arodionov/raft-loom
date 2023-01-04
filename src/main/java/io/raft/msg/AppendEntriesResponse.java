package io.raft.msg;
public record AppendEntriesResponse(Integer term, Boolean success) {
}
