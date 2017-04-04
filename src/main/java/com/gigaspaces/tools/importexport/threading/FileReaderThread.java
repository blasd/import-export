package com.gigaspaces.tools.importexport.threading;

import com.gigaspaces.client.WriteModifiers;
import com.gigaspaces.tools.importexport.config.ExportConfiguration;
import com.gigaspaces.tools.importexport.io.CustomObjectInputStream;
import com.gigaspaces.tools.importexport.lang.SpaceClassDefinition;
import com.gigaspaces.tools.importexport.lang.VersionSafeDescriptor;
import org.openspaces.core.GigaSpace;

import java.io.*;
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
        	GZIPInputStream zis = new GZIPInputStream(new BufferedInputStream(new FileInputStream(getFullFilePath())));
        	CustomObjectInputStream input = new CustomObjectInputStream(zis);
            try {
                validateFileNameAndType(input.readUTF());
                SpaceClassDefinition classDefinition = (SpaceClassDefinition) input.readObject();
                VersionSafeDescriptor typeDescriptor = classDefinition.getTypeDescriptor();
                space.getTypeManager().registerTypeDescriptor(typeDescriptor.toSpaceTypeDescriptor());

                int recordCount = 0;

                Collection<Object> spaceInstances = new ArrayList<Object>();

                Object record;

                do {
                    record = tryReadNextObject(input);

                    if(record != null) {
                        spaceInstances.add(classDefinition.toInstance((HashMap<String, Object>) record));
                        recordCount++;
                    }

                    if((spaceInstances.size() > 0 && (spaceInstances.size() % config.getBatch() == 0)) || (spaceInstances.size() > 0 && record == null)){
                        flush(spaceInstances);
                    }
                } while(record != null);

                output.setCount(recordCount);
            } finally {
            	input.close();
            	zis.close();
            }
        } catch (Exception ex) {
            output.setException(ex);
        }

        output.stop();
        return output;
    }

    private Object tryReadNextObject(CustomObjectInputStream input) throws Exception {
        Object output = null;
        try {
            output = input.readObject();
        } catch (EOFException ex) {

        } catch (OptionalDataException ex){

        }

        return output;
    }

    private void validateFileNameAndType(String className) {
        if(!className.equals(this.className)){
            throw new SecurityException(String.format("File name prefix does not match the encoded class name (case-sensitive). File name (prefix): [%s] Serialized class name: [%s]", this.className, className));
        }
    }

    private void flush(Collection<Object> spaceInstances) {
        space.writeMultiple(spaceInstances.toArray(), WriteModifiers.ONE_WAY);
        spaceInstances.clear();
    }

    public String getFullFilePath() {
        return config.getDirectory() + File.separator + fileName;
    }
}
