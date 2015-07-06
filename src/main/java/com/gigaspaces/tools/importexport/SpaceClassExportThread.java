package com.gigaspaces.tools.importexport;

import com.gigaspaces.client.iterator.GSIteratorConfig;
import com.gigaspaces.client.iterator.IteratorScope;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.index.SpaceIndex;
import com.gigaspaces.tools.importexport.serial.SerialAudit;
import com.gigaspaces.tools.importexport.serial.SerialList;
import com.gigaspaces.tools.importexport.serial.SerialMap;
import com.j_spaces.core.client.GSIterator;
import org.openspaces.core.GigaSpace;

import java.io.*;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static com.gigaspaces.tools.importexport.SpaceClassExportTask.*;

public class SpaceClassExportThread extends Thread {

    private final static java.util.logging.Logger logger = java.util.logging.Logger.getLogger(Constants.LOGGER_COMMON);

    private GigaSpace space;
    private File file;
    private String className;
    private Integer batch = 1000;
    private SerialAudit lines;

    public SpaceClassExportThread(GigaSpace space, File file, String className, Integer batch) {
        this.space = space;
        this.file = file;
        this.className = className;
        this.batch = batch;
        this.lines = new SerialAudit();
    }

    public SerialAudit getMessage() {

        return lines;
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
            }
            else
                logger.warning(cnfe.getMessage());
        } catch (InstantiationException ie) { logger.warning(ie.getMessage());
        } catch (IllegalAccessException iae) { logger.warning(iae.getMessage());
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

    @Override
    public void run() {

        GZIPOutputStream zos = null;
        ObjectOutputStream oos = null;
        try {
            Object template = getClassTemplate(className);
            if (template != null) {
                String type = (SpaceDocument.class.isInstance(template) ? Type.DOC.getValue() : Type.CLASS.getValue());
                logger.info("reading space " + type + " : " + className);
                lines.add("reading space " + type + " : " + className);
                Integer count = space.count(template);

                if (count > 0) {
                    logger.info("space partition contains " + count + " objects");
                    lines.add("space partition contains " + count + " objects");

                    // create the output file stream
                    zos = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
                    oos = new ObjectOutputStream(zos);

                    logger.info("writing to file : " + file.getAbsolutePath());
                    lines.add("writing to file : " + file.getAbsolutePath());

                    // write some header data
                    oos.writeUTF(SpaceDocument.class.isInstance(template) ? DOCUMENT : className);
                    oos.writeInt(count);
                    // space document needs to write type descriptor
                    if (Type.DOC.getValue().equals(type))
                        oos.writeUTF(className);

                    // we could serialize *all* type descriptors
                    oos.writeObject(getTypeDescriptorMap(className));

                    // get the objects and write them out
                    GSIteratorConfig config = new GSIteratorConfig();
                    config.setBufferSize(batch).setIteratorScope(IteratorScope.CURRENT);
                    Collection<Object> templates = new LinkedList<Object>();
                    templates.add(template);
                    GSIterator iterator = null;

                    try {
                        iterator = new GSIterator(space.getSpace(), templates, config);

                        logger.info("read " + count + " objects from space partition");
                        lines.add("read " + count + " objects from space partition");

                        Long start = System.currentTimeMillis();
                        while (iterator.hasNext()) {
                            oos.writeObject(iterator.next());
                        }
                        Long duration = (System.currentTimeMillis() - start);

                        logger.info("export operation took " + duration + " millis");
                        lines.add("export operation took " + duration + " millis");
                    } catch (Exception e) {
                        logger.info("import exception = " + e);
                        lines.add("import exception = " + e);
                        e.printStackTrace();
                    }

                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }   finally {
            try {
                zos.finish();
                oos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
