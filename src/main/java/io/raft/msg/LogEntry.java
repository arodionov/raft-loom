package io.raft.msg;

public record LogEntry(Integer term, String command) {
}
