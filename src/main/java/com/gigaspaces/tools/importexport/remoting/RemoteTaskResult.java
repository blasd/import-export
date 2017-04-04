package com.gigaspaces.tools.importexport.remoting;

import com.gigaspaces.tools.importexport.threading.ThreadAudit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

public class RemoteTaskResult implements Serializable {

    private static final long serialVersionUID = 3976667752496312168L;
    private String hostName;
    private Long processId;
    private Integer partitionId;
    private Collection<Exception> exceptions;
    private Collection<ThreadAudit> audits;
    private long start;
    private long stop;

    public RemoteTaskResult() {
        exceptions = new ArrayList<Exception>();
        audits= new ArrayList<ThreadAudit>();
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public Long getProcessId() {
        return processId;
    }

    public void setProcessId(Long processId) {
        this.processId = processId;
    }

    public Integer getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(Integer partitionId) {
        this.partitionId = partitionId;
    }

    public Collection<Exception> getExceptions() {
        return exceptions;
    }

    public void setExceptions(Collection<Exception> exception) {
        this.exceptions = exception;
    }

    public Collection<ThreadAudit> getAudits() {
        return audits;
    }

    public void setAudits(Collection<ThreadAudit> audits) {
        this.audits = audits;
    }

    public void start() {
        this.start = System.currentTimeMillis();
    }

    public void stop() {
        this.stop = System.currentTimeMillis();
    }

    public long getElapsedTime() {
        return this.stop - this.start;
    }
}

