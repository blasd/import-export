package com.gigaspaces.tool.importexport;

import com.gigaspaces.tools.importexport.model.Person;
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
        gigaSpace = startSpace("mySpace", 4);
    }

    @Test
    public void testExportImport() throws InterruptedException {
//        int count = 1000;
        for (int i = 0; i < 1000; i++){
            gigaSpace.write(new Person(i, "name" + i, i + 1));
        }
        int count = countPersons();
        System.out.println("persons = " + count);
        SpaceDataImportExportMain.main("-e -l 10.23.11.212 -s mySpace -g pavlo -d /tmp/gs -n 3".split(" "));
        System.out.println("EXPORT COMPLETED");
        undeploySpace("mySpace");
        Thread.sleep(5000);
        gigaSpace = startSpace("mySpace1", 3);
        Assert.assertEquals("", 0, countPersons());
        SpaceDataImportExportMain.main("-i -l 10.23.11.212 -s mySpace1 -g pavlo -d /tmp/gs".split(" "));
        Assert.assertEquals("", count, countPersons());
        System.out.println("persons = " + countPersons());
    }

    private int countPersons() {
        return gigaSpace.count(new Person());
    }

    @After
    public void tearDown(){
        undeploySpace("mySpace1");
    }

    private void undeploySpace(String spaceName) {
        admin.getProcessingUnits().getNames().get(spaceName).undeploy();
    }

    private GigaSpace startSpace(String spaceName, Integer instances){
        gsms = admin.getGridServiceManagers();
        gsms.waitFor(1);
        ProcessingUnit processingUnit = gsms.
                deploy(new SpaceDeployment(spaceName).numberOfInstances(instances).numberOfBackups(1));
        Space space = processingUnit.waitForSpace();
        space.waitFor(instances);
        return space.getGigaSpace();
    }

}
