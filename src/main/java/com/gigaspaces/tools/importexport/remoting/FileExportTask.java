package com.gigaspaces.tools.importexport.remoting;

import com.gigaspaces.tools.importexport.config.ExportConfiguration;
import com.gigaspaces.tools.importexport.config.SpaceConnectionFactory;
import com.gigaspaces.tools.importexport.threading.FileCreatorThread;
import com.gigaspaces.tools.importexport.threading.ThreadAudit;
import com.j_spaces.core.IJSpace;
import com.j_spaces.core.admin.IRemoteJSpaceAdmin;
import com.j_spaces.core.admin.SpaceRuntimeInfo;
import org.openspaces.admin.Admin;
import org.openspaces.admin.gsc.GridServiceContainer;
import org.openspaces.admin.pu.ProcessingUnit;
import org.openspaces.admin.pu.ProcessingUnitInstance;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoAware;
import org.openspaces.core.executor.Task;
import org.openspaces.core.executor.TaskGigaSpace;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

public class FileExportTask implements Task<RemoteTaskResult>, Serializable, ClusterInfoAware {

    private static final long serialVersionUID = 7132089756006051447L;
    private static final String JAVA_LANG_OBJECT = "java.lang.Object";

    @TaskGigaSpace
    private GigaSpace space;

    private Integer newPartitionCount;
    private ExportConfiguration config;
    private ClusterInfo clusterInfo;

    public FileExportTask(Integer newPartitionCount, ExportConfiguration config) {
        this.newPartitionCount = newPartitionCount;
        this.config = config;
    }

    @Override
    public RemoteTaskResult execute() throws Exception {
        RemoteTaskResult output = new RemoteTaskResult();
        try {
            configureOutput(output);
            Collection<String> classNames = getClassList(space.getSpace());

            Collection<Callable<ThreadAudit>> threads = new ArrayList<>();

            for (String className : classNames) {
                if(JAVA_LANG_OBJECT.equals(className)) continue;
                
                for (int x = 1; x <= newPartitionCount; x++) {
                    threads.add(new FileCreatorThread(space, config, className, this.clusterInfo.getInstanceId(), x, newPartitionCount));
                }
            }

            waitOnThreads(output, classNames.size(), threads);

        } catch(Exception ex){
            output.setException(ex);
        }

        return output;
    }

    private void waitOnThreads(RemoteTaskResult taskResult, int classCount, Collection<Callable<ThreadAudit>> threads) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(classCount);
        List<Future<ThreadAudit>> futures = executorService.invokeAll(threads);

        while(!futures.isEmpty()){
            Future<ThreadAudit> threadAuditFuture = futures.get(0);
            if(threadAuditFuture.isDone() || threadAuditFuture.isCancelled()){
                try {
                    ThreadAudit threadAudit = threadAuditFuture.get();

                    if(threadAudit != null)
                        taskResult.getAudits().add(threadAudit);
                } catch (ExecutionException ex){
                    ex.getCause().printStackTrace();
                }

                futures.remove(0);
            } else {
                Thread.sleep(1000);
            }
        }
    }

    private Collection<String> getClassList(IJSpace space) throws RemoteException {
        Collection<String> output = new ArrayList<>();
        output.addAll(config.getClasses());

        if(output.isEmpty()){
            IRemoteJSpaceAdmin javaSpaceAdmin = (IRemoteJSpaceAdmin)space.getAdmin();
            SpaceRuntimeInfo runtimeInfo = javaSpaceAdmin.getRuntimeInfo();
            output.addAll(runtimeInfo.m_ClassNames);
        }

        return output;
    }

    private void configureOutput(RemoteTaskResult output) throws Exception {
        output.setPartitionId(this.clusterInfo.getInstanceId());
        output.setAudits(new ArrayList<ThreadAudit>());

        SpaceConnectionFactory connections = new SpaceConnectionFactory(config);
        try {
            Admin spaceAdmin = connections.createAdmin();
            ProcessingUnit processingUnit = spaceAdmin.getProcessingUnits().waitFor(space.getName());
            ProcessingUnitInstance thisInstance = null;

            for (ProcessingUnitInstance instance : processingUnit.getInstances()) {
                ClusterInfo clusterInfo = instance.getClusterInfo();
                if (clusterInfo.getInstanceId().equals(clusterInfo.getInstanceId())) {
                    thisInstance = instance;
                }
            }

            if (thisInstance == null) {
                throw new IllegalStateException("Processing unit instance not found. Attempted partitionId id: " + clusterInfo.getInstanceId());
            }

            GridServiceContainer gridServiceContainer = thisInstance.getGridServiceContainer();
            output.setHostName(gridServiceContainer.getMachine().getHostName());
            output.setProcessId(gridServiceContainer.getVirtualMachine().getDetails().getPid());
        } catch (Exception ex){
            throw ex;
        }
        finally {
            connections.close();
        }
    }

    @Override
    public void setClusterInfo(ClusterInfo clusterInfo) {
        this.clusterInfo = clusterInfo;
    }
}

