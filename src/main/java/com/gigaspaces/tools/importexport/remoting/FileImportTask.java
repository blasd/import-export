package com.gigaspaces.tools.importexport.remoting;

import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import com.gigaspaces.tools.importexport.Constants;
import com.gigaspaces.tools.importexport.config.ExportConfiguration;
import com.gigaspaces.tools.importexport.threading.FileReaderThread;
import com.gigaspaces.tools.importexport.threading.ThreadAudit;

import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;

public class FileImportTask extends AbstractFileTask {
    private static final long serialVersionUID = -8411361196455200746L;

    public FileImportTask(ExportConfiguration config) {
        super(config);
    }

    @Override
    public RemoteTaskResult execute() throws Exception {
        RemoteTaskResult output = new RemoteTaskResult();
        try {
            configureOutput(output);
            Collection<Map.Entry<String, String>> filesToProcess = evaluateFilesToProcess();
            Collection<Callable<ThreadAudit>> threads = new ArrayList<>();

            for (Map.Entry<String, String> map : filesToProcess) {
                threads.add(new FileReaderThread(space, config, map.getKey(), map.getValue()));
            }

            waitOnThreads(output, 50, threads);

        } catch(Exception ex){
            output.setException(ex);
        }

        return output;
    }

    private Collection<Map.Entry<String, String>> evaluateFilesToProcess() {
        Collection<Map.Entry<String, String>> output = new ArrayList<>();

        File fileDirectory = new File(config.getDirectory());
        String[] fileList = fileDirectory.list();

        for (String name : fileList) {
            String className = name.split(getSuffixRegex())[0];

            if(!className.equals(name)){
                // Match was found and class name separated
                if((config.getClasses() != null && !config.getClasses().isEmpty())
                        && !config.getClasses().contains(className)) {
                    continue;
                }

                warmUpSpace(className);
                output.add(new AbstractMap.SimpleEntry<>(className, name));
            }
        }
        return output;
    }

    private void warmUpSpace(String className) {
        try {
            Class<?> aClass = Class.forName(className);
            space.getTypeManager().registerTypeDescriptor(aClass);
        } catch(ClassNotFoundException ex){
            // Ignore, it might be a space document.
        }
    }

    private String getSuffixRegex() {
        return ".\\d+." + clusterInfo.getInstanceId() + Constants.FILE_EXTENSION;
    }
}
