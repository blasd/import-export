package com.gigaspaces.tools.importexport.config;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.CommaParameterSplitter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ExportConfiguration implements Serializable {

    private static final long serialVersionUID = 435362013613840214L;

    @Parameter(names = { "-o", "--operation"}, description = "A flag indicating whether an export or import will be performed. (e.g. import/export)", required = true)
    private Operation operation = Operation.EXPORT;

    @Parameter(names = {"-s", "--space"}, description = "Name of the target space to perform either an export or import.", required = true)
    private String name = "space";

    @Parameter(names = {"-b", "--batch"}, description = "Performance option to batch records retrieved from the space.")
    private Integer batch = 1000;

    @Parameter(names = {"-d", "--directory"}, description = "A full path to the directory containing either previously exported files, or where the exported files should be placed.", required = true)
    private String directory;

    @Parameter(names = {"-l", "--locators"}, description = "A comma separated list of XAP lookup locators for the target grid.", splitter = CommaParameterSplitter.class)
    private List<String> locators = new ArrayList<>();

    @Parameter(names = {"-g", "--groups"}, description = "A comma separated list of XAP lookup groups for the target grid.")
    private List<String> groups = new ArrayList<>();

    @Parameter(names = {"-c", "--classes"}, description = "A comma separated list of class names to export or import into the grid. The class names are case sensitive.")
    private List<String> classes = new ArrayList<>();

    @Parameter(names = {"-p", "--partitions"}, description = "A comma separated list of partitions that will be exported or imported.")
    private List<Integer> partitions = new ArrayList<>();

    @Parameter(names = {"-u", "--username"}, description = "Specifies an XAP username with read and execute privileges. Required when connecting to a secured grid.")
    private String username;

    @Parameter(names = {"-a", "--password"}, description = "Specifies an XAP password corresponding to the specified XAP username. Required when connecting to a secured grid.")
    private String password;

    @Parameter(names = {"-n", "--number"}, description = "Relevant only when exporting data for use in a grid with a different partition count (i.e. Exporting data from a 6 partition grid to 2 partition grid or vice versa.)")
    private Integer newPartitionCount;

    @Parameter(names = {"--thread-sleep"}, description = "Number of milliseconds to sleep between checks for task completion.")
    private Integer threadSleepMilliseconds = 1000;

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

    public Integer getThreadSleepMilliseconds() {
        return threadSleepMilliseconds;
    }

    public void setThreadSleepMilliseconds(Integer threadSleepMilliseconds) {
        this.threadSleepMilliseconds = threadSleepMilliseconds;
    }
}