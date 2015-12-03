package com.gigaspaces.tools.importexport;

import com.gigaspaces.annotation.pojo.FifoSupport;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.metadata.index.SpacePathIndex;
import com.gigaspaces.tools.importexport.model.Person;
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

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ImportExportTest {

    public static final int INITIAL_PARTITION_COUNT = 2;
    public static final int TARGET_PARTITION_COUNT = 3;
    private Admin admin;

    private GridServiceManagers gsms;

    private GigaSpace gigaSpace;

    private String group = "test";

    @Before
    public void init(){
        admin = new AdminFactory().addGroup(group).create();
        gigaSpace = startSpace("mySpace", INITIAL_PARTITION_COUNT);
    }

    @Test
    public void testExportImport() throws InterruptedException {
        for (int i = 0; i < 10000; i++){
            gigaSpace.write(new Person(i, "name" + i, i + 1));
        }
        int count = countPersons();
        System.out.println("persons = " + count);
        String tempDir = System.getProperty("java.io.tmpdir") + File.separator + "gs";
        String exportArgs = "-e -s mySpace -g " + group + " -n " + TARGET_PARTITION_COUNT + " -d " + tempDir;
        System.out.println("EXPORT ARGS = " + exportArgs);
        Program.main(exportArgs.toString().split(" "));
        System.out.println("EXPORT COMPLETED");
        undeploySpace("mySpace");
        Thread.sleep(5000);
        gigaSpace = startSpace("mySpace1", TARGET_PARTITION_COUNT);
        Assert.assertEquals("", 0, countPersons());
        String importArgs = "-i -s mySpace1 -g " + group + " -d " + tempDir;
        System.out.println("IMPORT ARGS = " + importArgs);
        Program.main(importArgs.split(" "));
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
        gsms.waitFor(1, 10, TimeUnit.SECONDS);
        ProcessingUnit processingUnit = gsms.
                deploy(new SpaceDeployment(spaceName).numberOfInstances(instances).numberOfBackups(1));
        Space space = processingUnit.waitForSpace();
        space.waitFor(instances);
        return space.getGigaSpace();
    }


    public void testing() {
        SpaceTypeDescriptorBuilder builder = new SpaceTypeDescriptorBuilder("");
//        builder.addFixedProperty()
//        spaceTypeDescriptorBuilder.addFixedProperty()

//            builder.supports
//          builder.get
//        spaceTypeDescriptorBuilder.addIndex(new SpacePathIndex("", ));
//        spaceTypeDescriptorBuilder.supportsDynamicProperties()
//
//                SpaceTypeDescriptor d = null;
//
//        Map<String, SpaceIndex> indexes = d.getIndexes();
//
//        String idPropertyName = d.getIdPropertyName();
//        String routingPropertyName = d.getRoutingPropertyName();
//        FifoSupport fifoSupport = d.getFifoSupport();
//        d.supports
//        d.getStorageType()
//        fifoSupport.

    }

}
