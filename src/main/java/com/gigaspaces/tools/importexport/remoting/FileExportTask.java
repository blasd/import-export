package com.gigaspaces.tools.importexport.remoting;

import com.gigaspaces.tools.importexport.Constants;
import com.gigaspaces.tools.importexport.config.ExportConfiguration;
import com.gigaspaces.tools.importexport.threading.FileCreatorThread;
import com.gigaspaces.tools.importexport.threading.ThreadAudit;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.admin.IRemoteJSpaceAdmin;
import com.j_spaces.core.admin.SpaceRuntimeInfo;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

public class FileExportTask extends AbstractFileTask {
    private static Logger logger = Logger.getLogger(Constants.LOGGER_NAME);
    private static final long serialVersionUID = 7132089756006051447L;
    private static final String JAVA_LANG_OBJECT = "java.lang.Object";

    private Integer newPartitionCount;

    public FileExportTask(Integer newPartitionCount, ExportConfiguration config) {
        super(config);
        this.newPartitionCount = newPartitionCount;
    }

    @Override
    public Collection<Callable<ThreadAudit>> execute(RemoteTaskResult taskResult) throws Exception {
        Collection<Callable<ThreadAudit>> output = new ArrayList<Callable<ThreadAudit>>();
        Collection<String> classNames = getClassList(space.getSpace());

        for (String className : classNames) {
            if(JAVA_LANG_OBJECT.equals(className)) continue;

            for (int x = 1; x <= newPartitionCount; x++) {
                logger.info(String.format("Defining thread to process [Class: %s, Target Partition: %s]", className, x));
                output.add(new FileCreatorThread(space, config, className, this.clusterInfo.getInstanceId(), x, newPartitionCount));
            }
        }

        return output;
    }

    private Collection<String> getClassList(IJSpace space) throws RemoteException {
        Collection<String> output = new ArrayList<String>();
        output.addAll(config.getClasses());

        if(output.isEmpty()){
            IRemoteJSpaceAdmin javaSpaceAdmin = (IRemoteJSpaceAdmin)space.getAdmin();
            SpaceRuntimeInfo runtimeInfo = javaSpaceAdmin.getRuntimeInfo();
            output.addAll(runtimeInfo.m_ClassNames);
        }

        return output;
    }
}

