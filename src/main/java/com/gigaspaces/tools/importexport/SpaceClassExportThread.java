package com.gigaspaces.tools.importexport;

import com.gigaspaces.client.iterator.GSIteratorConfig;
import com.gigaspaces.client.iterator.IteratorScope;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.tools.importexport.serial.Audit;
import com.gigaspaces.tools.importexport.serial.SerialList;
import com.gigaspaces.tools.importexport.serial.SerialMap;
import com.j_spaces.core.client.GSIterator;
import net.jini.core.entry.UnusableEntryException;
import org.openspaces.core.GigaSpace;

import java.io.*;
import java.lang.reflect.Field;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static com.gigaspaces.tools.importexport.ExportImportTask.*;

public class SpaceClassExportThread extends AbstractSpaceThread{

    private String className;

    private Integer futurePartitionId;

    private Integer clusterSize;

    public SpaceClassExportThread(GigaSpace space, File file, String className, Integer batch, Integer futurePartitionID, Integer clusterSize) {
        this.space = space;
        this.file = file;
        this.className = className;
        this.futurePartitionId = futurePartitionID;
        this.batch = batch;
        this.clusterSize = clusterSize;
    }

    @Override
    protected Audit performOperation() throws Exception{
        try (GZIPOutputStream zos = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
             ObjectOutputStream oos = new ObjectOutputStream(zos)){

            Object template = getClassTemplate(className);
            if (template != null) {
                String type = (SpaceDocument.class.isInstance(template) ? Type.DOC.getValue() : Type.CLASS.getValue());
                logInfoMessage("reading space " + type + " : " + className);
                //Integer count = space.count(template);
                Integer count = countObjects(template, getTypeDescriptorMap(className));
                if (count > 0) {
                    logInfoMessage("space partition contains " + count + " objects");
                    logInfoMessage("writing to file : " + file.getAbsolutePath());
                    // write some header data
                    oos.writeUTF(SpaceDocument.class.isInstance(template) ? DOCUMENT : className);
                    oos.writeInt(count);
                    // space document needs to write type descriptor
                    if (Type.DOC.getValue().equals(type))
                        oos.writeUTF(className);

                    // we could serialize *all* type descriptors
                    SerialMap typeDescriptorMap = getTypeDescriptorMap(className);

                    logInfoMessage("TYPE DESCRIPTOR");
                    for (String key :typeDescriptorMap.keySet()){
                        logInfoMessage("KEY = " + key + " VALUE = " + typeDescriptorMap.get(key));
                    }
                    oos.writeObject(typeDescriptorMap);

                    // get the objects and write them out
                    GSIteratorConfig config = new GSIteratorConfig();
                    config.setBufferSize(batch).setIteratorScope(IteratorScope.CURRENT);
                    Collection<Object> templates = new LinkedList<>();
                    templates.add(template);
                    GSIterator iterator = new GSIterator(space.getSpace(), templates, config);
                    logInfoMessage("read " + count + " objects from space partition");
                    Long start = System.currentTimeMillis();
                    Field field = Class.forName(className).getDeclaredField((String) typeDescriptorMap.get(ROUTING));
                    field.setAccessible(true);

                    while (iterator.hasNext()) {
                        Object next = iterator.next();
                        Object routingValue = field.get(next);
                        if ((routingValue.hashCode() % clusterSize) + 1 == futurePartitionId){
                            logInfoMessage("ROUTING VALUE = " + routingValue + " CLUSTER SIZE = " + clusterSize + " PARTITION_ID = " + futurePartitionId);
                            oos.writeObject(next);
                        }
                    }
                    Long duration = (System.currentTimeMillis() - start);
                    logInfoMessage("export operation took " + duration + " millis");


                }
            }
            return result;
        }
    }

    private Integer countObjects(Object template, SerialMap typeDescriptorMap) throws RemoteException, UnusableEntryException, ClassNotFoundException, IllegalAccessException, NoSuchFieldException {
        int count = 0;
        GSIteratorConfig config = new GSIteratorConfig();
        config.setBufferSize(batch).setIteratorScope(IteratorScope.CURRENT);
        Collection<Object> templates = new LinkedList<>();
        templates.add(template);
        GSIterator iterator = new GSIterator(space.getSpace(), templates, config);
        Long start = System.currentTimeMillis();
        Field field = Class.forName(className).getDeclaredField((String) typeDescriptorMap.get(ROUTING));
        field.setAccessible(true);

        while (iterator.hasNext()) {
            Object next = iterator.next();
            Object routingValue = field.get(next);
            if ((routingValue.hashCode() % clusterSize) + 1 == futurePartitionId){
                logInfoMessage("ROUTING VALUE = " + routingValue + " CLUSTER SIZE = " + clusterSize + " PARTITION_ID = " + futurePartitionId + " COUNT " + count++);
            }
        }
        return count;
    }

    private Object getClassTemplate(String className) {
        Object template = null;
        try {
            template = Class.forName(className).newInstance();
            logger.fine("returning " + template.getClass().getSimpleName() + " (SpaceDocument)" );
        } catch (ClassNotFoundException cnfe) {
            SpaceTypeDescriptor descriptor = space.getTypeManager().getTypeDescriptor(className);
            // right way to check for space document
            if (! descriptor.isConcreteType()) {
                template = new SpaceDocument(className);
                logger.fine("returning SpaceDocument");
            }   else {
                logger.warning(cnfe.getMessage());
            }
        } catch (InstantiationException | IllegalAccessException ie) {
            logger.warning(ie.getMessage());
        }
        return template;
    }

    private SerialMap getTypeDescriptorMap(String className) {
        SerialMap documentMap = new SerialMap();
        SpaceTypeDescriptor type = space.getTypeManager().getTypeDescriptor(className);

        if (type.getIdPropertyName() != null)
            documentMap.put(SPACEID, type.getIdPropertyName());
        if (type.getRoutingPropertyName() != null)
            documentMap.put(ROUTING, type.getRoutingPropertyName());

        SerialMap indexMap = new SerialMap();
        Map<String, SpaceIndex> indexes = type.getIndexes();
        for (String key : indexes.keySet()) {

            // despite the fact that the importer won't create indexes
            // on the routing or spaceid we're not going to create the index
            if (type.getIdPropertyName() != null && type.getIdPropertyName().equals(key))
                continue;
            if (type.getRoutingPropertyName() != null && type.getRoutingPropertyName().equals(key))
                continue;

            // @SpaceIndex
            if (! indexMap.containsKey(indexes.get(key).getIndexType().name()))
                indexMap.put(indexes.get(key).getIndexType().name(), new SerialList());

            ((SerialList) indexMap.get(indexes.get(key).getIndexType().name())).add(key);
        }
        // always write this out, even if it's empty
        documentMap.put(INDEX, indexMap);

        return documentMap;
    }

}
