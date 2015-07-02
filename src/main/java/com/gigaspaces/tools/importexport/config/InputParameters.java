package com.gigaspaces.tools.importexport.config;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.CommaParameterSplitter;
import java.util.Arrays;

import java.util.ArrayList;
import java.util.List;

public class InputParameters {

    @Parameter(names = {"-s", "--space"}, description = "the name of the space")
    private String name = "space";

    @Parameter(names = {"-e", "--export"}, description = "performs space class export")
    private Boolean export = false;

    @Parameter(names = {"-i", "--import"}, description = "performs space class import")
    private Boolean imp = true;

    @Parameter(names = {"-t", "--test"}, description = "performs space class import")
    private Boolean test = false;

    @Parameter(names = {"-b", "--batch"}, description = "performs a sanity check")
    private Integer batch = 1000;

    @Parameter(names = {"-l", "--locators"}, description = "the names of lookup services hosts - comma separated", splitter = CommaParameterSplitter.class)
    private List<String> locators = Arrays.asList("localhost");

    @Parameter(names = {"-g", "--groups"}, description = "the names of lookup groups - comma separated")
    private List<String> groups = new ArrayList<String>();

    @Parameter(names = {"-c", "--classes"}, description = "the classes whose objects to import/export - comma separated")
    private List<String> classes = new ArrayList<String>();

    @Parameter(names = {"-p", "--partitions"}, description = "the partition(s) to restore - comma separated")
    private List<Integer> partitions = new ArrayList<Integer>();

    @Parameter(names = {"-u", "--username"}, description = "The username when connecting to a secured space.")
    private String username;

    @Parameter(names = {"-a", "--password"}, description = "The password when connecting to a secured space.")
    private String password;

    public String getName() {
        return name;
    }

    public Boolean getExport() {
        return export;
    }

    public Boolean getImp() {
        return imp;
    }

    public Boolean getTest() {
        return test;
    }

    public Integer getBatch() {
        return batch;
    }

    public List<String> getLocators() {
        return locators;
    }

    public List<String> getGroups() {
        return groups;
    }

    public List<String> getClasses() {
        return classes;
    }

    public List<Integer> getPartitions() {
        return partitions;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
