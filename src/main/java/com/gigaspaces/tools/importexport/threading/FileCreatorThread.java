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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.zip.GZIPOutputStream;

public class FileCreatorThread implements Callable<ThreadAudit> {
    private GigaSpace space;
    private ExportConfiguration config;
    private String className;
    private Integer partitionId;
    private int newPartitionId;
    private Integer newPartitionSchema;

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
        output.start();

        try {
            SpaceClassDefinition definition = SpaceClassDefinition.create(space, className);
            List objects = readSpaceObjects(space, definition);

            if (objects != null && objects.size() > 0) {
                output.setCount(objects.size());

                String filePath = config.getDirectory() + File.separator + output.getFileName();
                try (GZIPOutputStream zos = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(filePath)));
                     ObjectOutputStream oos = new ObjectOutputStream(zos)) {
                    oos.writeUTF(className);
                    oos.writeInt(objects.size());
                    oos.writeObject(definition);

                    for (Object instance : objects) {
                        oos.writeObject(definition.toMap(instance));
                    }
                }
            }
        } catch(Exception ex){
            output.setException(ex);
        }

        output.stop();
        return output;
    }

    private List readSpaceObjects(GigaSpace space, SpaceClassDefinition definition) throws Exception {
        List output = new ArrayList();

        GSIteratorConfig gsIteratorConfig = new GSIteratorConfig();
        gsIteratorConfig.setBufferSize(config.getBatch()).setIteratorScope(IteratorScope.CURRENT);
        Collection<Object> templates = new ArrayList<>();
        templates.add(definition.toTemplate());

        GSIterator gsIterator = new GSIterator(space.getSpace(), templates, gsIteratorConfig);

        while(gsIterator.hasNext()){
            Object instance = gsIterator.next();
            Object routingValue = definition.getRoutingValue(instance);

            if((routingValue.hashCode() % this.newPartitionSchema) + 1 == newPartitionId)
                output.add(instance);
        }

        return output;
    }
}

