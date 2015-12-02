package com.gigaspaces.tools.importexport.remoting;

import com.gigaspaces.tools.importexport.threading.ThreadAudit;

import java.io.Serializable;
import java.util.Collection;

public class RemoteTaskResult implements Serializable {

    private static final long serialVersionUID = 3976667752496312168L;
    private String hostName;
    private Long processId;
    private Integer partitionId;
    private Exception exception;
    private Collection<ThreadAudit> audits;

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

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    public Collection<ThreadAudit> getAudits() {
        return audits;
    }

    public void setAudits(Collection<ThreadAudit> audits) {
        this.audits = audits;
    }
}

