package com.gigaspaces.tools.importexport.threading;

import java.io.Serializable;

public class ThreadAudit implements Serializable {
    private static final long serialVersionUID = 7139939706151192186L;

    private long start;
    private long stop;
    private long count;
    private Exception exception;
    private String fileName;

    public ThreadAudit(String fileName){
        this.fileName = fileName;
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
        return this.fileName;
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
