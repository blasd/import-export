package com.gigaspaces.tool.importexport;

import com.gigaspaces.tool.importexport.model.Person;
import com.gigaspaces.tools.importexport.SpaceDataImportExportMain;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.openspaces.admin.Admin;
import org.openspaces.admin.AdminFactory;
import org.openspaces.admin.gsm.GridServiceManagers;
import org.openspaces.admin.pu.ProcessingUnit;
import org.openspaces.admin.space.Space;
import org.openspaces.admin.space.SpaceDeployment;
import org.openspaces.core.GigaSpace;

public class ImportExportTest {

    private Admin admin;

    private GridServiceManagers gsms;

    private GigaSpace gigaSpace;

    @Before
    public void init(){
        admin = new AdminFactory().addGroup("pavlo").create();
        gigaSpace = startSpace();
    }

    @Test
    public void testExportImport(){
        for (int i = 0; i < 100; i++){
            gigaSpace.write(new Person(i, "name" + i, i + 1));
        }
        int count = countPersons();
        System.out.println("persons = " + count);
        SpaceDataImportExportMain.main("-e -l 10.23.11.212 -s mySpace -g pavlo".split(" "));
        System.out.println("EXPORT COMPLETED");
        undeploySpace();
        gigaSpace = startSpace();
        Assert.assertEquals("", 0, countPersons());
        SpaceDataImportExportMain.main("-i -l 10.23.11.212 -s mySpace -g pavlo".split(" "));
        Assert.assertEquals("", count, countPersons());
        System.out.println("persons = " + countPersons());
    }

    private int countPersons() {
        return gigaSpace.count(new Person());
    }

    @After
    public void tearDown(){
        undeploySpace();
    }

    private void undeploySpace() {
        admin.getProcessingUnits().getNames().get("mySpace").undeploy();
    }

    private GigaSpace startSpace(){
        gsms = admin.getGridServiceManagers();
        gsms.waitFor(1);
        ProcessingUnit processingUnit = gsms.
                deploy(new SpaceDeployment("mySpace").numberOfInstances(1).numberOfBackups(1));

        //wait for the instances to start
        Space space = processingUnit.waitForSpace();
        space.waitFor(2);
        return space.getGigaSpace();
    }

}
