package com.gigaspaces.tools.importexport;

import com.gigaspaces.tools.importexport.config.InputParameters;
import com.google.common.base.Joiner;

import java.util.Arrays;
import java.util.List;

import static org.apache.commons.collections.CollectionUtils.*;

public class SpaceURLBuilder {

    private InputParameters config;

    public SpaceURLBuilder(InputParameters config) {
        this.config = config;
    }

    public String buildSpaceURL(){
        String spaceUrl = "jini://*/*/" + config.getName() + "?";
        List<String> groups = config.getGroups();
        List<String> locators = config.getLocators();
        if (isEmpty(groups) && isEmpty(locators)){
            locators = Arrays.asList("localhost");
        }
        if (isNotEmpty(locators)){
            spaceUrl += "locators=" + Joiner.on(",").join(locators);
        }
        if (isNotEmpty(groups)){
            if (isNotEmpty(locators)){
                spaceUrl += "&";
            }
            spaceUrl += "groups=" + Joiner.on(",").join(groups);
        }
        return spaceUrl;
    }

}
