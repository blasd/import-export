package com.gigaspaces.tools.importexport.remoting;

import com.gigaspaces.tools.importexport.threading.ThreadAudit;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

public class RemoteTaskResult implements Serializable {

    private static final long serialVersionUID = 3976667752496312168L;
    private String hostName;
    private Long processId;
    private Integer partitionId;
    private Collection<String> files;
    private Exception exception;
    private Collection<String> messages;
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

    @Deprecated
    public Collection<String> getFiles() {
        return files;
    }

    @Deprecated
    public void setFiles(Collection<String> files) {
        this.files = files;
    }

    public Exception getException() {
        return exception;
    }

    public void setException(Exception exception) {
        this.exception = exception;
    }

    @Deprecated
    public void setMessages(Collection<String> messages) {
        this.messages = messages;
    }

    @Deprecated
    public Collection<String> getMessages() {
        return messages;
    }

    public Collection<ThreadAudit> getAudits() {
        return audits;
    }

    public void setAudits(Collection<ThreadAudit> audits) {
        this.audits = audits;
    }
}

