package com.gigaspaces.tools.importexport;

import java.io.File;
import java.io.FilenameFilter;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

import com.gigaspaces.tools.importexport.config.InputParameters;

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

public class SpaceClassExportTask implements DistributedTask<SerialList, List<String>>, ClusterInfoAware {
	
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
	
	private static String COLON = ":";
	private static String DOT = ".";
		
	private final static java.util.logging.Logger logger = java.util.logging.Logger.getLogger(Constants.LOGGER_COMMON);
	
	private List<String> classNames = new ArrayList<>();
	private Boolean export;
	private Integer batch;
	private SerialAudit audit = new SerialAudit();
	
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

	public SpaceClassExportTask(InputParameters config) {
        this.export = config.getExport();
        this.classNames.addAll(config.getClasses());
        this.batch = config.getBatch();
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

    private void handleImport() {
        File[] files = new File(DOT).listFiles(new ImportClassFileFilter(clusterInfo.getInstanceId()));
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
	
	/* (non-Javadoc)
	 * @see com.gigaspaces. {
	 * async.AsyncResultsReducer#reduce(java.util.List)
	 */
	@Override
	public List<String> reduce(List<AsyncResult<SerialList>> args) throws Exception {
		List<String> result = new ArrayList<String>();
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

	private void writeObjects(List<String> classList) {

        ExecutorService executor = Executors.newFixedThreadPool(classList.size());
		List<SpaceClassExportThread> threadList = new ArrayList<SpaceClassExportThread>();
		Integer partitionId = clusterInfo.getInstanceId();
		for (String className : classList) {
			File file = new File(className + DOT + partitionId + SUFFIX);
            logMessage("starting export TO FILE " + file.getAbsolutePath());
            SpaceClassExportThread operation = new SpaceClassExportThread(gigaSpace, file, className, batch);
			logMessage("starting export thread for " + className);
            executor.submit(operation);
			threadList.add(operation);
		}
        logMessage("waiting for " + classList.size() + " import operations to complete-complete");
        waitForExecution(executor);
		for (SpaceClassExportThread thread : threadList) {
			for (String line : thread.getMessage()){
                (audit).add(line);
            }
		}
		logMessage("finished writing " + classList.size() + " classes");
	}

	private void readObjects(List<String> fileNames) {
		List<SpaceClassImportThread> threadList = new ArrayList<SpaceClassImportThread>();
		Integer partitionId = clusterInfo.getInstanceId();
        ExecutorService executor = Executors.newFixedThreadPool(fileNames.size());
		for (String fileName : fileNames) {
			// we're being passed a file instead of a class name
			File file = new File(fileName);
			logMessage("importing class " + getClassNameFromImportFile(file) + " into partition " + partitionId);
			SpaceClassImportThread operation = new SpaceClassImportThread(gigaSpace, file, 1000);
			threadList.add(operation);
            executor.submit(operation);
		}
		logMessage("waiting for " + fileNames.size() + " import operations to complete");
        waitForExecution(executor);
		for (SpaceClassImportThread thread : threadList) {
			for (String line : thread.getMessage()) {
				audit.add(line, false);
			}
		}
		logMessage("finished reading " + fileNames.size() + " files");
	}

    private void waitForExecution(ExecutorService executor) {
        executor.shutdown();
        while (!executor.isTerminated()){}
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
