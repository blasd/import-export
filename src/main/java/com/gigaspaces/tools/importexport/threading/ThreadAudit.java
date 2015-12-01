package com.gigaspaces.tools.importexport.threading;

import java.io.Serializable;

public class ThreadAudit implements Serializable {
    private static final long serialVersionUID = 7139939706151192186L;

    private String className;
    private Integer partitionId;
    private int newPartitionId;
    private long start;
    private long stop;
    private long count;
    private Exception exception;

    public ThreadAudit(String className, Integer partitionId, int newPartitionId) {
        this.className = className;
        this.partitionId = partitionId;
        this.newPartitionId = newPartitionId;
    }

    public void start() {
        this.start = System.currentTimeMillis();
    }

    public void stop() {
        this.stop = System.currentTimeMillis();
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getCount() {
        return count;
    }

    public String getFileName() {
        return className + "." + partitionId + "." + newPartitionId + ".ser.gz";
    }

    public long getTime() {
        return stop - start;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public Exception getException() {
        return exception;
    }
}
