package com.gigaspaces.tools.importexport.threading;

import com.gigaspaces.client.iterator.GSIteratorConfig;
import com.gigaspaces.client.iterator.IteratorScope;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.tools.importexport.AbstractSpaceThread;
import com.gigaspaces.tools.importexport.config.ExportConfiguration;
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
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static com.gigaspaces.tools.importexport.ExportImportTask.*;

@Deprecated
public class SpaceClassExportThread extends AbstractSpaceThread implements Serializable {
    private static final long serialVersionUID = 0l;

    private static final String SUFFIX = ".ser.gz";
    private static final String DOT = ".";
    private final String fileName;
    private ExportConfiguration config;
    private String className;
    private Integer partitionId;

    private Integer futurePartitionId;
    private Integer newPartitionCount;

    public SpaceClassExportThread(GigaSpace space, ExportConfiguration config, String className, Integer partitionId, Integer futurePartitionId, Integer newPartitionCount){
        this.className = className;
        this.partitionId = partitionId;
        this.futurePartitionId = futurePartitionId;
        this.newPartitionCount = newPartitionCount;
        this.space = space;
        this.config = config;
        this.batch = config.getBatch();
        this.fileName = className + DOT + SUFFIX;
        this.filePath = config.getDirectory() + "/" + this.fileName;
    }

    @Override
    protected Audit performOperation() throws Exception {
        Audit output = new Audit();
        output.setFileName(this.fileName);
        output.setClassName(className);
        output.start();

        Object template = getClassTemplate(className);
        if (template != null) {
            String type = (SpaceDocument.class.isInstance(template) ? Type.DOC.getValue() : Type.CLASS.getValue());
            List filteredObjects = filterObjectsByRoutingKey(template, getTypeDescriptorMap(className));
            int count = filteredObjects.size();
            output.setCount(count);

            if (count > 0) {
                try(GZIPOutputStream zos = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(filePath)));
                ObjectOutputStream oos = new ObjectOutputStream(zos)){
                    oos.writeUTF(SpaceDocument.class.isInstance(template) ? DOCUMENT : className);
                    oos.writeInt(count);

                    // space document needs to write type descriptor
                    if (Type.DOC.getValue().equals(type))
                        oos.writeUTF(className);

                    // we could serialize *all* type descriptors
                    SerialMap typeDescriptorMap = getTypeDescriptorMap(className);
                    oos.writeObject(typeDescriptorMap);

                    for (Object obj : filteredObjects){
                        oos.writeObject(obj);
                    }
                }
            }
        }

        output.stop();
        return output;
    }

    private List filterObjectsByRoutingKey(Object template, SerialMap typeDescriptorMap) throws RemoteException, UnusableEntryException, ClassNotFoundException, IllegalAccessException, NoSuchFieldException {
        List result = new ArrayList();
        GSIteratorConfig config = new GSIteratorConfig();
        config.setBufferSize(batch).setIteratorScope(IteratorScope.CURRENT);
        Collection<Object> templates = new LinkedList<>();
        templates.add(template);
        GSIterator iterator = new GSIterator(space.getSpace(), templates, config);
        Field field = Class.forName(className).getDeclaredField((String) typeDescriptorMap.get(ROUTING));
        field.setAccessible(true);

        while (iterator.hasNext()) {
            Object next = iterator.next();
            Object routingValue = field.get(next);
            if ((routingValue.hashCode() % this.newPartitionCount) + 1 == futurePartitionId){
                result.add(next);
            }
        }
        return result;
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
