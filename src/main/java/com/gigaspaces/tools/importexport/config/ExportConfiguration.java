package com.gigaspaces.tools.importexport.config;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.CommaParameterSplitter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ExportConfiguration implements Serializable {

    private static final long serialVersionUID = 435362013613840214L;

    @Parameter(names = { "-o", "--operation"}, description = "", required = true)
    private Operation operation = Operation.EXPORT;

    @Parameter(names = {"-s", "--space"}, description = "Name of the space to operate", required = true)
    private String name = "space";

    @Parameter(names = {"-b", "--batch"}, description = "The batch size - default is 1000")
    private Integer batch = 1000;

    @Parameter(names = {"-d", "--directory"}, description = "Read-from/write-to directory", required = true)
    private String directory;

    @Parameter(names = {"-l", "--locators"}, description = "The names of lookup services hosts - comma separated", splitter = CommaParameterSplitter.class)
    private List<String> locators = new ArrayList<>();

    @Parameter(names = {"-g", "--groups"}, description = "The names of lookup groups - comma separated")
    private List<String> groups = new ArrayList<>();

    @Parameter(names = {"-c", "--classes"}, description = "The classes whose objects to import/export - comma separated")
    private List<String> classes = new ArrayList<>();

    @Parameter(names = {"-p", "--partitions"}, description = "The partition(s) to restore - comma separated")
    private List<Integer> partitions = new ArrayList<>();

    @Parameter(names = {"-u", "--username"}, description = "The username when connecting to a secured space.")
    private String username;

    @Parameter(names = {"-a", "--password"}, description = "The password when connecting to a secured space.")
    private String password;

    @Parameter(names = {"-n", "--number"}, description = "Number of partitions to export.")
    private Integer newPartitionCount;

    public Integer getNewPartitionCount() {
        return newPartitionCount;
    }

    public void setNewPartitionCount(Integer newPartitionCount) {
        this.newPartitionCount = newPartitionCount;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getBatch() {
        return batch;
    }

    public void setBatch(Integer batch) {
        this.batch = batch;
    }

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public List<String> getLocators() {
        return locators;
    }

    public void setLocators(List<String> locators) {
        this.locators = locators;
    }

    public List<String> getGroups() {
        return groups;
    }

    public void setGroups(List<String> groups) {
        this.groups = groups;
    }

    public List<String> getClasses() {
        return classes;
    }

    public void setClasses(List<String> classes) {
        this.classes = classes;
    }

    public List<Integer> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<Integer> partitions) {
        this.partitions = partitions;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }
}
