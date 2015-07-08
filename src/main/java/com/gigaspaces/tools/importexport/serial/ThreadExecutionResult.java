package com.gigaspaces.tools.importexport.serial;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ThreadExecutionResult implements Serializable {

    private List<String> audit = new ArrayList<>();

    public String hostname;

    public String containerName;

    public Integer containerPid;

    public List<String> getAudit() {
        return audit;
    }

    public void setAudit(List<String> audit) {
        this.audit = audit;
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public String getContainerName() {
        return containerName;
    }

    public void setContainerName(String containerName) {
        this.containerName = containerName;
    }

    public Integer getContainerPid() {
        return containerPid;
    }

    public void setContainerPid(Integer containerPid) {
        this.containerPid = containerPid;
    }
}
