package com.gigaspaces.tools.importexport.remoting;

import com.gigaspaces.tools.importexport.config.ExportConfiguration;
import com.gigaspaces.tools.importexport.config.SpaceConnectionFactory;
import com.gigaspaces.tools.importexport.threading.FileCreatorThread;
import com.gigaspaces.tools.importexport.threading.FileReaderThread;
import com.gigaspaces.tools.importexport.threading.ThreadAudit;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by skyler on 12/1/2015.
 */
public abstract class AbstractFileTask implements Task<RemoteTaskResult>, Serializable, ClusterInfoAware {
    private static final long serialVersionUID = -8253008691316469029L;
    protected ExportConfiguration config;
    protected ClusterInfo clusterInfo;

    /** LRMI Class Loading Hack **/
    private FileCreatorThread creatorThread;
    private FileReaderThread readerThread;
    /** LRMI Class Loading Hack **/

    @TaskGigaSpace
    protected GigaSpace space;

    public AbstractFileTask(ExportConfiguration config){
        this.config = config;
    }

    @Override
    public void setClusterInfo(ClusterInfo clusterInfo){
        this.clusterInfo = clusterInfo;
    }

    protected final void configureOutput(RemoteTaskResult output) throws Exception {
        output.setPartitionId(this.clusterInfo.getInstanceId());
        output.setAudits(new ArrayList<ThreadAudit>());

        SpaceConnectionFactory connections = new SpaceConnectionFactory(config);
        try {
            Admin spaceAdmin = connections.createAdmin();
            ProcessingUnit processingUnit = spaceAdmin.getProcessingUnits().waitFor(space.getName());
            ProcessingUnitInstance thisInstance = null;

            for (ProcessingUnitInstance instance : processingUnit.getInstances()) {
                ClusterInfo clusterInfo = instance.getClusterInfo();
                if (this.clusterInfo.getInstanceId().equals(clusterInfo.getInstanceId())) {
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

    protected final void waitOnThreads(RemoteTaskResult taskResult, int classCount, Collection<Callable<ThreadAudit>> threads) throws Exception {
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
}
