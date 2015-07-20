package com.gigaspaces.tools.importexport;


import com.gigaspaces.tools.importexport.SpaceURLBuilder;
import com.gigaspaces.tools.importexport.config.InputParameters;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class SpaceUrlTest {

    private InputParameters config;

    @Before
    public void setUp(){
        config = new InputParameters();
        config.setName("space");
    }

    @Test
    public void testLocators(){
        config.setLocators(Arrays.asList("localhost", "10.23.11.212"));
        String spaceUrl = new SpaceURLBuilder(config).buildSpaceURL();
        System.out.println(spaceUrl);
        Assert.assertEquals("jini://*/*/space?locators=localhost,10.23.11.212", spaceUrl);
    }

    @Test
    public void testGroups(){
        config.setGroups(Arrays.asList("test", "production"));
        String spaceUrl = new SpaceURLBuilder(config).buildSpaceURL();
        Assert.assertEquals("jini://*/*/space?groups=test,production", spaceUrl);
    }

    @Test
    public void testLocatorsAndGroups(){
        config.setGroups(Arrays.asList("test", "production"));
        config.setLocators(Arrays.asList("localhost", "10.23.11.212"));
        String spaceUrl = new SpaceURLBuilder(config).buildSpaceURL();
        Assert.assertEquals("jini://*/*/space?locators=localhost,10.23.11.212&groups=test,production", spaceUrl);
    }

    public void testDefault(){
        config.setName("space");
        String spaceUrl = new SpaceURLBuilder(config).buildSpaceURL();
        Assert.assertEquals("jini://*/*/space?locators=localhost", spaceUrl);
    }
}
