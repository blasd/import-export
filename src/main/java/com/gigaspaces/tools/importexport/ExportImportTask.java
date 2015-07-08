package com.gigaspaces.tools.importexport;

import java.io.File;
import java.io.FilenameFilter;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Level;

import com.gigaspaces.tools.importexport.config.InputParameters;

import com.gigaspaces.tools.importexport.serial.ThreadExecutionResult;
import org.openspaces.admin.Admin;
import org.openspaces.admin.AdminFactory;
import org.openspaces.admin.gsc.GridServiceContainer;
import org.openspaces.admin.pu.ProcessingUnit;
import org.openspaces.admin.pu.ProcessingUnitInstance;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoAware;
import org.openspaces.core.executor.DistributedTask;
import org.openspaces.core.executor.TaskGigaSpace;

import com.gigaspaces.async.AsyncResult;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.logger.Constants;
import com.gigaspaces.tools.importexport.serial.SerialAudit;
import com.gigaspaces.tools.importexport.serial.SerialList;
import com.j_spaces.core.admin.IRemoteJSpaceAdmin;
import com.j_spaces.core.admin.SpaceRuntimeInfo;

public class ExportImportTask implements DistributedTask<SerialList, List<String>>, ClusterInfoAware {
	
	private static final long serialVersionUID = 5257838144063003892L;
	/**
	 * 
	 */
	public static String OBJECT = "java.lang.Object";
	public static String DOCUMENT = SpaceDocument.class.getName();
	private static String SUFFIX = ".ser.gz";
	
	public static String SPACEID = "@SpaceId";
	public static String ROUTING = "@SpaceRouting";
	public static String INDEX = "@SpaceIndex";
	
	private static String DOT = ".";
		
	private final static java.util.logging.Logger logger = java.util.logging.Logger.getLogger(Constants.LOGGER_COMMON);
	
	private List<String> classNames = new ArrayList<>();
	private Boolean export;
	private Integer batch;
	private SerialAudit audit = new SerialAudit();
	private String storagePath;
    private InputParameters config;

	// we don't really use this other than to get the groups and locators
	@TaskGigaSpace
	private transient GigaSpace gigaSpace;

	// injection doesn't work
    private ClusterInfo clusterInfo;		

    public enum Type {
    	DOC("document"),
    	CLASS("class");
    	private String value;
    	private Type(String value) { this.value = value; }
    	public String getValue() { return value; }
    }

	public ExportImportTask(InputParameters config) {
        this.export = config.getExport();
        this.classNames.addAll(config.getClasses());
        this.batch = config.getBatch();
        this.storagePath = config.getDirectory();
        this.config = config;
	}

    private Admin initializeAdmin(InputParameters config) {
        AdminFactory adminFactory = new AdminFactory();
        if (config.getLocators() != null) {
            for (String locator : config.getLocators()) {
                adminFactory.addLocator(locator);
            }
        }
        if (config.getGroups() != null) {
            for (String group : config.getGroups()) {
                adminFactory.addGroup(group);
            }
        }
        if (config.getUsername() != null) {
            String user = config.getUsername();
            String password = config.getPassword();
            adminFactory.credentials(user, password);
        }
        return adminFactory.createAdmin();
    }

    /* (non-Javadoc)
     * @see org.openspaces.core.executor.Task#execute()
     */
	@Override
	public SerialList execute() throws Exception {
		if (export) {
            handleExport();
		}   else {
            handleImport();
		}
		return audit;
	}

    /* (non-Javadoc)
     * @see com.gigaspaces. {
     * async.AsyncResultsReducer#reduce(java.util.List)
     */
    @Override
    public List<String> reduce(List<AsyncResult<SerialList>> args) throws Exception {
        List<String> result = new ArrayList<>();
        for (AsyncResult<SerialList> arg : args) {
            if (arg.getException() == null && arg.getResult() != null) {
                for (String str : arg.getResult())
                    result.add(str);
            }   else {
                result.add("exception:" + arg.getException().getMessage());
                arg.getException().printStackTrace();
            }
        }
        return result;
    }

    private void handleImport() {
        File[] files = new File(storagePath).listFiles(new ImportClassFileFilter(clusterInfo.getInstanceId()));
        List<String> fileNames = new ArrayList<>();
        for (File file : files) {
            if (classNames.isEmpty() || classNames.contains(getClassNameFromImportFile(file))) {
                fileNames.add(file.toString());
            }
        }
        logMessage("importer found " + fileNames.size() + " files");
        if (fileNames.size() > 0){
            readObjects(fileNames);
        }
    }

    private void handleExport() throws RemoteException {
        IRemoteJSpaceAdmin remoteAdmin = (IRemoteJSpaceAdmin) gigaSpace.getSpace().getAdmin();
        if (! classNames.isEmpty()) {
            for (String className : classNames) {
                SpaceRuntimeInfo runtimeInfo = remoteAdmin.getRuntimeInfo(className);
                if (runtimeInfo != null) {
                    logClassInstancesCount(runtimeInfo);
                }
                else {
                    classNames.remove(className);
                    logger.warning("space class export task - class: " + className + " was not found!");
                }
            }
            logMessage("confirmed " + classNames.size() + " classes");
        }   else {
            Object classList[] = remoteAdmin.getRuntimeInfo().m_ClassNames.toArray();
            logClassInstancesCount(remoteAdmin, classList);
            for (Object clazz : classList) {
                logger.fine(clazz.toString());
                if (! clazz.toString().equals(OBJECT)){
                    classNames.add(clazz.toString());
                }
            }
            logMessage("found " + classNames.size() + " classes");
        }

        if (classNames.size() > 0){
            writeObjects(classNames);
        }
    }

    private String getClassNameFromImportFile(File file) {
		StringBuilder buffer = new StringBuilder();
		String[] parts = file.getName().split("\\.");
		// class.name.#.ser.gz - we don't care about the last three
		for (int f = 0; f < parts.length - 3; f++) {
			buffer.append((buffer.length() > 0 ? "." : "")).append(parts[f]);
		}
		return buffer.toString();
	}

    private void writeObjects(List<String> classList) {
        ExecutorService executor = Executors.newFixedThreadPool(classList.size());
        Integer partitionId = clusterInfo.getInstanceId();
        List<Callable<ThreadExecutionResult>> threads = new ArrayList<>();
        for (String className : classList) {
            File file = new File(storagePath + File.separator + className + DOT + partitionId + SUFFIX);
            logMessage("starting export TO FILE " + file.getAbsolutePath());
            SpaceClassExportThread operation = new SpaceClassExportThread(gigaSpace, file, className, batch);
            logMessage("starting export thread for " + className);
            threads.add(operation);
        }
        executeOperation(executor, threads);
        logMessage("finished writing " + classList.size() + " classes");
    }

    private void readObjects(List<String> fileNames) {
		List<Callable<ThreadExecutionResult>> threadList = new ArrayList<>();
		Integer partitionId = clusterInfo.getInstanceId();
        ExecutorService executor = Executors.newFixedThreadPool(fileNames.size());
		for (String fileName : fileNames) {
			File file = new File(fileName);
			logMessage("importing class " + getClassNameFromImportFile(file) + " into partition " + partitionId);
			SpaceClassImportThread operation = new SpaceClassImportThread(gigaSpace, file, 1000);
			threadList.add(operation);
		}
		executeOperation(executor, threadList);
        logMessage("finished reading " + fileNames.size() + " files");
	}


    private void executeOperation(ExecutorService executor, List<Callable<ThreadExecutionResult>> threads) {
        try {
            List<Future<ThreadExecutionResult>> futures = executor.invokeAll(threads);
            logMessage("waiting for " + threads.size() + " import operations to complete-complete");
            // waitForExecution(executor);
            for (Future<ThreadExecutionResult> future : futures){
                try {
                    ThreadExecutionResult threadExecutionResult = future.get();
                    audit.addAll(threadExecutionResult.getAudit());
                }   catch (ExecutionException e) {
                    logMessage("execution EXCEPTION " + e);
                    Admin admin = initializeAdmin(config);
                    admin.getSpaces().waitFor(gigaSpace.getName());
                    ProcessingUnit processingUnit = admin.getProcessingUnits().waitFor(gigaSpace.getName());
                    GridServiceContainer gsc = findGSC(processingUnit);
                    if (gsc != null){
                        logMessage("HOSTNAME = " + gsc.getMachine().getHostName());
                        logMessage("GSC UID = " + gsc.getUid());
                        logMessage("PID = " + gsc.getVirtualMachine().getDetails().getPid());
                    }   else {
                        logMessage("UNABLE TO RETRIEVE GSC INFORMATION");
                    }
                    admin.close();
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private GridServiceContainer findGSC(ProcessingUnit pu) {
        ProcessingUnitInstance[] processingUnitInstances = pu.getInstances();
        logMessage("PUI = " + processingUnitInstances.length);
        for (ProcessingUnitInstance pui : processingUnitInstances){
            ClusterInfo adminClusterInfo = pui.getClusterInfo();
            logMessage("CLUSTER INFO = " + adminClusterInfo.getName() + " instance " + clusterInfo.getInstanceId());
            if (clusterInfo.getName().equals(adminClusterInfo.getName()) && clusterInfo.getInstanceId().equals(adminClusterInfo.getInstanceId())){
                return pui.getGridServiceContainer();
            }
        }
        return null;
    }

    private void logClassInstancesCount(IRemoteJSpaceAdmin remoteAdmin, Object[] classList) throws RemoteException {
        if (logger.isLoggable(Level.FINE)) {
            List<?> numOfEntries = remoteAdmin.getRuntimeInfo().m_NumOFEntries;
            for (int c = 0; c < classList.length; c++)
                logger.fine(classList[c] + " has " + numOfEntries.get(c).toString() + " objects");
        }
    }

    private void logClassInstancesCount(SpaceRuntimeInfo runtimeInfo) {
        if (logger.isLoggable(Level.FINE)) {
            List<?> numOfEntries = runtimeInfo.m_NumOFEntries;
            for (int c = 0; c < runtimeInfo.m_ClassNames.size(); c++)
                logger.fine(runtimeInfo.m_ClassNames.get(c) + " has " + numOfEntries.get(c).toString() + " objects");
        }
    }

	@Override
	public void setClusterInfo(ClusterInfo clusterInfo) {
		this.clusterInfo = clusterInfo;
	}

    private void logMessage(String message){
        logger.info(message);
        audit.add(message);
    }

    private class ImportClassFileFilter implements FilenameFilter {

        private Integer partitionId;

        public ImportClassFileFilter(Integer partitionId) {

            this.partitionId = partitionId;
        }

        @Override
        public boolean accept(File dir, String name) {
            return name.endsWith(partitionId + SUFFIX);
        }
    }

}
