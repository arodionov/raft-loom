package io.raft.msg;

import java.util.ArrayList;
import java.util.List;

public class Log<T> {

    private int lastIndex = 0;

    private List<T> log = new ArrayList<>();

    public T get(int i) {
        if (lastIndex - 1 < i) throw new IndexOutOfBoundsException("lastIndex is: " + lastIndex + " but i is: " + i);
        else return log.get(i);
    }

    public void add(int i, T entry) {
        if (i == lastIndex) {
            lastIndex++;
            log.add(entry);
        } else if (i < lastIndex) {
            lastIndex = i + 1;
            log.set(i, entry);
        }
    }

    public List<T> entries() {
        return log;
    }

    public int lastIndex() {
        return lastIndex;
    }
}
