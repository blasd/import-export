package com.gigaspaces.tools.importexport;

import com.gigaspaces.tools.importexport.config.InputParameters;
import com.google.common.base.Joiner;

import java.util.Arrays;
import java.util.List;


public class SpaceURLBuilder {

    private InputParameters config;

    public SpaceURLBuilder(InputParameters config) {
        this.config = config;
    }

    public String buildSpaceURL(){
        String spaceUrl = "jini://*/*/" + config.getName() + "?";
        List<String> groups = config.getGroups();
        List<String> locators = config.getLocators();
        boolean emptyLocators = locators.isEmpty();
        boolean emptyGroups = groups.isEmpty();
        if (emptyGroups && emptyLocators){
            locators = Arrays.asList("localhost");
        }
        if (!emptyLocators){
            spaceUrl += "locators=" + Joiner.on(",").join(locators);
        }
        if (!emptyGroups){
            if (!emptyLocators){
                spaceUrl += "&";
            }
            spaceUrl += "groups=" + Joiner.on(",").join(groups);
        }
        return spaceUrl;
    }

}
