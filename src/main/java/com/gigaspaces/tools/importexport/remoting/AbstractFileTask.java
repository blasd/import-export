package com.gigaspaces.tools.importexport.remoting;

import com.gigaspaces.tools.importexport.config.ExportConfiguration;
import com.gigaspaces.tools.importexport.config.SpaceConnectionFactory;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by skyler on 12/1/2015.
 */
public abstract class AbstractFileTask implements Task<RemoteTaskResult>, Serializable, ClusterInfoAware, Callable<HashMap<String, Object>> {
    private static final long serialVersionUID = -8253008691316469029L;
    public static final String HOST_NAME_KEY = "__HOST_NAME";
    public static final String PROCESS_ID_KEY = "__PROCESS_ID";
    private static final String EXCEPTION_KEY = "__EXCEPTION";
    protected final LRMIClassLoadHacker hacker = new LRMIClassLoadHacker();

    protected ExportConfiguration config;
    protected ClusterInfo clusterInfo;

    @TaskGigaSpace
    protected GigaSpace space;

    public AbstractFileTask(ExportConfiguration config){
        this.config = config;
    }

    @Override
    public void setClusterInfo(ClusterInfo clusterInfo){
        this.clusterInfo = clusterInfo;
    }

    @Override
    public final RemoteTaskResult execute(){
        RemoteTaskResult output = new RemoteTaskResult();
        output.start();
        output.setPartitionId(this.clusterInfo.getInstanceId());

        ExecutorService executorService = null;
        try {
            executorService = Executors.newSingleThreadExecutor();
            Future<HashMap<String, Object>> gatheringMachineInfo = executorService.submit(this);
            Collection<Callable<ThreadAudit>> threads = execute(output);
            processThreadResults(output, threads);

            HashMap<String, Object> machineInfo = gatheringMachineInfo.get();

            if(!machineInfo.containsKey(EXCEPTION_KEY)) {
                output.setHostName((String) machineInfo.get(HOST_NAME_KEY));
                output.setProcessId((Long) machineInfo.get(PROCESS_ID_KEY));
            } else {
                output.getExceptions().add((Exception)machineInfo.get(EXCEPTION_KEY));
            }
        } catch(Exception ex){
            output.getExceptions().add(ex);
        } finally {
            if(executorService != null) executorService.shutdown();
        }

        output.stop();
        return output;
    }

    protected abstract Collection<Callable<ThreadAudit>> execute(RemoteTaskResult output) throws Exception;

    @Override
    public HashMap<String, Object> call() {
        HashMap<String, Object> output = new HashMap<String, Object>();
        SpaceConnectionFactory connections = new SpaceConnectionFactory(config);

        try {
            Admin spaceAdmin = connections.createAdmin();
            ProcessingUnit processingUnit = spaceAdmin.getProcessingUnits().waitFor(config.getProcessingUnitName(), 60, TimeUnit.SECONDS);
            processingUnit.waitFor(processingUnit.getTotalNumberOfInstances());
            ProcessingUnitInstance thisInstance = null;

            for (ProcessingUnitInstance instance : processingUnit.getInstances()) {
                ClusterInfo clusterInfo = instance.getClusterInfo();
                if (this.clusterInfo.getInstanceId().equals(clusterInfo.getInstanceId()) && clusterInfo.getBackupId() == null) {
                    thisInstance = instance;
                }
            }

            if (thisInstance == null) {
                throw new IllegalStateException("Processing unit instance not found. Attempted partitionId id: " + clusterInfo.getInstanceId());
            }

            GridServiceContainer gridServiceContainer = thisInstance.getGridServiceContainer();
            output.put(HOST_NAME_KEY, gridServiceContainer.getMachine().getHostName());
            output.put(PROCESS_ID_KEY, gridServiceContainer.getVirtualMachine().getDetails().getPid());
        } catch (Exception ex){
            output.put(EXCEPTION_KEY, ex);
        }
        finally {
            try {
                connections.close();
            }catch(Exception ex){

            }
        }

        return output;
    }

    private void processThreadResults(RemoteTaskResult taskResult, Collection<Callable<ThreadAudit>> threads) throws Exception {
        if(config.getThreadCount() > 0){
            invokeAll(taskResult, threads);
        } else {
            invokeEach(taskResult, threads);
        }
    }

    private void invokeEach(RemoteTaskResult taskResult, Collection<Callable<ThreadAudit>> threads) throws Exception {
        for(Callable<ThreadAudit> thread : threads){
            ThreadAudit result = thread.call();

            if(result != null)
                taskResult.getAudits().add(result);
        }
    }

    private void invokeAll(RemoteTaskResult taskResult, Collection<Callable<ThreadAudit>> threads) throws Exception {
        ExecutorService executorService = null;

        try {
            executorService = Executors.newFixedThreadPool(config.getThreadCount(), Executors.privilegedThreadFactory());
            List<Future<ThreadAudit>> futures = executorService.invokeAll(threads);

            while (!futures.isEmpty()) {
                Future<ThreadAudit> threadAuditFuture = futures.get(0);
                if (threadAuditFuture.isDone() || threadAuditFuture.isCancelled()) {
                    try {
                        ThreadAudit threadAudit = threadAuditFuture.get();

                        if (threadAudit != null)
                            taskResult.getAudits().add(threadAudit);
                    } catch (ExecutionException ex) {
                        taskResult.getExceptions().add(ex);
                    }

                    futures.remove(0);
                } else {
                    Thread.sleep(config.getThreadSleepMilliseconds());
                }
            }
        } catch(Exception ex) {
            taskResult.getExceptions().add(ex);
        } finally {
            if(executorService != null)
                executorService.shutdown();
        }
    }
}
