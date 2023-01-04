package io.raft.msg;

import java.util.List;

public record AppendEntriesRequest(Integer term, Integer leaderId, Integer prevLogIndex, Integer prevLogTerm,
                                   List<LogEntry> entries, Integer leaderCommit) {
}
