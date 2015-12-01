package com.gigaspaces.tools.importexport.remoting;

import com.gigaspaces.tools.importexport.config.ExportConfiguration;
import org.openspaces.core.executor.Task;

import java.io.Serializable;

public class ImportFilesTask implements Task<RemoteTaskResult>, Serializable {

    public ImportFilesTask(Integer newPartitionCount, ExportConfiguration config) {

    }

    @Override
    public RemoteTaskResult execute() throws Exception {
        return null;
    }
}
