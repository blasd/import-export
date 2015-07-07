package com.gigaspaces.tools.importexport;

import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.metadata.index.SpaceIndexFactory;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.gigaspaces.tools.importexport.serial.SerialAudit;
import com.gigaspaces.tools.importexport.serial.SerialList;
import com.gigaspaces.tools.importexport.serial.SerialMap;
import org.openspaces.core.GigaSpace;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static com.gigaspaces.tools.importexport.ExportImportTask.*;

public class SpaceClassImportThread extends AbstractSpaceThread implements Runnable {

    public SpaceClassImportThread(GigaSpace space, File file, Integer batch) {
        this.space = space;
        this.file = file;
        this.batch = batch;
        this.lines = new SerialAudit();
    }

    @Override
    public void run() {
        ObjectInputStream input = null;
        try {
            GZIPInputStream zis = new GZIPInputStream(new BufferedInputStream(new FileInputStream(file)));
            input = new ObjectInputStream(zis);
            logInfoMessage("opened import file " + file.toString());
            String className = input.readUTF();
            Integer objectCount = input.readInt();
            String typeName = isDocument(className) ? input.readUTF() : className;
            SerialMap propertyMap = (SerialMap) input.readObject();
            registerTypeDescriptor(typeName, propertyMap);
            // we log classname, so set it up to reflect the space document type
            className = typeName + " (" + className + ")";

            logInfoMessage("found " + objectCount + " instances of " + className);
            List objectList = new ArrayList();
            Long start = System.currentTimeMillis();
            for (int i = 0; i < objectCount; i++) {
                objectList.add(input.readObject());
                if (i > 0 && (i % batch == 0 || i + 1 == objectCount)) {
                    space.writeMultiple(objectList.toArray(new Object[objectList.size()]));
                    if (i + 1 < objectCount) {
                        objectList.clear();
                    }
                }
            }
            Long duration = (System.currentTimeMillis() - start);
            logInfoMessage("import operation took " + duration + " millis");
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        } finally {
            if (input != null){
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void registerTypeDescriptor(String typeName, SerialMap typeMap) {
        SpaceTypeDescriptorBuilder typeBuilder = new SpaceTypeDescriptorBuilder(typeName);

        // is this document already registered?
        if (typeAlreadyRegistered(typeName)) {
            logInfoMessage("found type descriptor for " + typeName);
            return;
        }
        // create it if necessary
        logInfoMessage("creating type descriptor for " + typeName);

        // deal with spaceId, spaceRouting, indexes separately
        handleSpaceId(typeName, typeMap, typeBuilder);
        handleRouting(typeName, typeMap, typeBuilder);
        handleIndexes(typeName, typeMap, typeBuilder);
        // Register type:
        space.getTypeManager().registerTypeDescriptor(typeBuilder.create());
    }

    private void handleIndexes(String typeName, SerialMap typeMap, SpaceTypeDescriptorBuilder typeBuilder) {
        for (String propertyName : typeMap.keySet()) {
            if (INDEX.equals(propertyName)) {
                SerialMap indexMap = (SerialMap) typeMap.get(propertyName);
                for (String indexType : indexMap.keySet()) {
                    SpaceIndexType type = SpaceIndexType.valueOf(indexType);
                    SerialList indexes = (SerialList) indexMap.get(indexType);
                    for (String index : indexes) {
                        SpaceIndex spaceIndex = SpaceIndexFactory.createPropertyIndex(index, type);
                        try {
                            logger.fine("creating index " + spaceIndex.toString() + " for type " + typeName);
                            typeBuilder.addIndex(spaceIndex);
                        } catch (IllegalArgumentException iae) {
                            logger.warning("registerTypeDescriptor" + iae.getMessage());
                        }
                    }
                }
            }
        }
    }

    private boolean isDocument(String className) {
        return className.equals(DOCUMENT);
    }

    private void handleRouting(String typeName, SerialMap typeMap, SpaceTypeDescriptorBuilder typeBuilder) {
        if (typeMap.keySet().contains(ROUTING)) {
            logFineMessage("creating routing property " + typeMap.get(ROUTING) + " for type " + typeName);
            typeBuilder.routingProperty((String) typeMap.get(ROUTING));
        }
    }

    private void handleSpaceId(String typeName, SerialMap typeMap, SpaceTypeDescriptorBuilder typeBuilder) {
        if (typeMap.keySet().contains(SPACEID)) {
            logFineMessage("creating id property " + typeMap.get(SPACEID) + " for type " + typeName);
            typeBuilder.idProperty((String) typeMap.get(SPACEID));
        }
    }

    private boolean typeAlreadyRegistered(String typeName) {
        return space.getTypeManager().getTypeDescriptor(typeName) != null;
    }


}