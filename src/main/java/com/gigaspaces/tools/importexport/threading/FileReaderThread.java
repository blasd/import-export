package com.gigaspaces.tools.importexport.threading;

import com.gigaspaces.client.WriteModifiers;
import com.gigaspaces.tools.importexport.config.ExportConfiguration;
import com.gigaspaces.tools.importexport.lang.SpaceClassDefinition;
import com.gigaspaces.tools.importexport.lang.VersionSafeDescriptor;
import org.openspaces.core.GigaSpace;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.zip.GZIPInputStream;

/**
 * Created by skyler on 12/1/2015.
 */
public class FileReaderThread implements Callable<ThreadAudit> {
    private final GigaSpace space;
    private final ExportConfiguration config;
    private final String className;
    private final String fileName;

    public FileReaderThread(GigaSpace space, ExportConfiguration config, String className, String fileName) {
        this.space = space;
        this.config = config;
        this.className = className;
        this.fileName = fileName;
    }

    @Override
    public ThreadAudit call() throws Exception {
        ThreadAudit output = new ThreadAudit(fileName);
        output.start();

        try {
            try (GZIPInputStream zis = new GZIPInputStream(new BufferedInputStream(new FileInputStream(getFullFilePath())));
                 ObjectInputStream input = new ObjectInputStream(zis)) {

                input.readUTF(); // Throw away value this is just the class name and we already have it.
                int recordCount = input.readInt();
                output.setCount(recordCount);
                SpaceClassDefinition classDefinition = (SpaceClassDefinition) input.readObject();
                VersionSafeDescriptor typeDescriptor = classDefinition.getTypeDescriptor();
                space.getTypeManager().registerTypeDescriptor(typeDescriptor.toSpaceTypeDescriptor());

                Collection<Object> spaceInstances = new ArrayList<>();

                for(int x = 0; x < recordCount; x++){

                    spaceInstances.add(classDefinition.toInstance((HashMap<String, Object>) input.readObject()));

                    if((x % config.getBatch() == 1) || (x == recordCount - 1)){
                        flush(spaceInstances);
                    }
                }
            }
        } catch (Exception ex) {
            output.setException(ex);
        }

        output.stop();
        return output;
    }

    private void flush(Collection<Object> spaceInstances) {
        space.writeMultiple(spaceInstances.toArray(), WriteModifiers.ONE_WAY);
        spaceInstances.clear();
    }

    public String getFullFilePath() {
        return config.getDirectory() + File.separator + fileName;
    }
}
