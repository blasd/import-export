package com.gigaspaces.tools.importexport.remoting;

import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

import com.gigaspaces.tools.importexport.Constants;
import com.gigaspaces.tools.importexport.config.ExportConfiguration;
import com.gigaspaces.tools.importexport.threading.FileReaderThread;
import com.gigaspaces.tools.importexport.threading.ThreadAudit;

public class FileImportTask extends AbstractFileTask {
    private static final long serialVersionUID = -8411361196455200746L;
    
	private static final Logger LOGGER = Logger.getLogger(FileImportTask.class.getName());

    public FileImportTask(ExportConfiguration config) {
        super(config);
    }

    @Override
    public Collection<Callable<ThreadAudit>> execute(RemoteTaskResult taskResult) throws Exception {
        Collection<Callable<ThreadAudit>> output = new ArrayList<Callable<ThreadAudit>>();
        Collection<Map.Entry<String, String>> filesToProcess = evaluateFilesToProcess();

        for (Map.Entry<String, String> map : filesToProcess) {
            output.add(new FileReaderThread(space, config, map.getKey(), map.getValue()));
        }

        return output;
    }

    private Collection<Map.Entry<String, String>> evaluateFilesToProcess() {
        Collection<Map.Entry<String, String>> output = new ArrayList<Map.Entry<String, String>>();

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

				LOGGER.info("Adding for import: " + name);

               // preLoadTypeDescriptors(className, config);
                output.add(new AbstractMap.SimpleEntry<String, String>(className, name));
            }
        }
        return output;
    }

	private String getSuffixRegex() {
		if (true) {
			// By default, we restrict files from the matching partition: it requires -n to be correctly configured when
			// running the export
			return ".\\d+." + clusterInfo.getInstanceId() + Constants.FILE_EXTENSION;
		} else {
			// We enable loading from any partition. We do it to force loading from a cluster with a different typology
			return ".\\d+." + "\\d+" + Constants.FILE_EXTENSION;
		}
	}
}
