package com.gigaspaces.tools.importexport.threading;

import com.gigaspaces.client.iterator.GSIteratorConfig;
import com.gigaspaces.client.iterator.IteratorScope;
import com.gigaspaces.tools.importexport.Constants;
import com.gigaspaces.tools.importexport.config.ExportConfiguration;
import com.gigaspaces.tools.importexport.lang.SpaceClassDefinition;
import com.j_spaces.core.client.GSIterator;
import org.openspaces.core.GigaSpace;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

public class FileCreatorThread implements Callable<ThreadAudit> {
    private static Logger logger = Logger.getLogger(Constants.LOGGER_NAME);
    private GigaSpace space;
    private ExportConfiguration config;
    private String className;
    private Integer partitionId;
    private int newPartitionId;
    private Integer newPartitionSchema;
    private GZIPOutputStream zippedOutputStream;
    private ObjectOutputStream objectOutputStream;
    private boolean fileInitialized = false;

    public FileCreatorThread(GigaSpace space, ExportConfiguration config, String className, Integer partitionId, int newPartitionId, Integer newPartitionSchema) {
        this.space = space;
        this.config = config;
        this.className = className;
        this.partitionId = partitionId;
        this.newPartitionId = newPartitionId;
        this.newPartitionSchema = newPartitionSchema;
    }

    @Override
    public ThreadAudit call() throws Exception {
        ThreadAudit output = new ThreadAudit(className + "." + partitionId + "." + newPartitionId + Constants.FILE_EXTENSION);
        log(output, "Processing started.");
        output.start();

        try {
            SpaceClassDefinition definition = SpaceClassDefinition.create(space, config, className);
            int recordCount = iterateSpaceObjects(space, definition, output);
            log(output, "Found " + recordCount + " records.");
            output.setCount(recordCount);
        } catch (Exception ex) {
            log(output, "Exception: " + ex.getMessage());
            output.setException(ex);
        } finally {
            if(zippedOutputStream != null){
                zippedOutputStream.close();
            }

            if(objectOutputStream != null){
                objectOutputStream.close();
            }

            fileInitialized = false;
        }

        output.stop();
        return output;
    }

    private int iterateSpaceObjects(GigaSpace space, SpaceClassDefinition definition, ThreadAudit audit) throws Exception {
        int output = 0;

        GSIteratorConfig gsIteratorConfig = new GSIteratorConfig();
        gsIteratorConfig.setBufferSize(config.getBatch()).setIteratorScope(IteratorScope.CURRENT);
        Collection<Object> templates = new ArrayList<>();
        templates.add(definition.toTemplate());

        GSIterator gsIterator = new GSIterator(space.getSpace(), templates, gsIteratorConfig);

        while(gsIterator.hasNext()){
            Object instance = gsIterator.next();
            Object routingValue = definition.getRoutingValue(instance);

            int hashCode = adjustNegativeHashCodes(routingValue.hashCode());
            int realRoute = (hashCode % this.newPartitionSchema) + 1;

            if(realRoute == newPartitionId) {
                writeToFile(instance, definition, audit);
                output++;
            }
        }

        return output;
    }

    private int adjustNegativeHashCodes(int hashCode) {
        return hashCode == Integer.MIN_VALUE ? Integer.MAX_VALUE : Math.abs(hashCode);
    }

    private void writeToFile(Object instance, SpaceClassDefinition definition, ThreadAudit audit) throws Exception {
        ensureFileInitialized(definition, audit);
        objectOutputStream.writeObject(definition.toMap(instance));
    }

    private void ensureFileInitialized(SpaceClassDefinition definition, ThreadAudit audit) throws Exception {
        if(!fileInitialized) {
            String filePath = config.getDirectory() + File.separator + audit.getFileName();
            zippedOutputStream = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(filePath)));
            objectOutputStream = new ObjectOutputStream(zippedOutputStream);
            objectOutputStream.writeUTF(className);
            objectOutputStream.writeObject(definition);
            fileInitialized = true;
        }
    }

    private void log(ThreadAudit output, String message) {
        logger.info(String.format("%s -- %s", output.getFileName(), message));
    }
}

