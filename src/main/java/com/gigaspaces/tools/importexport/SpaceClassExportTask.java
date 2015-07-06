package com.gigaspaces.tools.importexport;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;
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
	
	private List<String> classNames;
	private Boolean export;
	private Integer batch;
	private SerialAudit audit;
	
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
    
	public SpaceClassExportTask() {
		
		classNames = new ArrayList<String>();
		audit = new SerialAudit();
	}
	
	public SpaceClassExportTask(Boolean export) {

		this();
		this.export = export;
	}

	public SpaceClassExportTask(InputParameters config) {
		this(config.getClasses(), config.getExport(), config.getBatch());
	}
	

	public SpaceClassExportTask(List<String> className, Boolean export, Integer batch) {

		this(export);
		this.classNames.addAll(classNames);
		this.batch = batch;
	}

	/* (non-Javadoc)
	 * @see org.openspaces.core.executor.Task#execute()
	 */
	@Override
	public SerialList execute() throws Exception {

		// writes the partition id with the thread id in the logs
//		audit.setPartition(clusterInfo.getInstanceId());
		
		if (export) {
			// get a list of classes and the number of entries of each class
			IRemoteJSpaceAdmin remoteAdmin = (IRemoteJSpaceAdmin) gigaSpace.getSpace().getAdmin();
			if (! classNames.isEmpty()) {
				for (String className : classNames) {
					SpaceRuntimeInfo runtimeInfo = remoteAdmin.getRuntimeInfo(className);
					if (runtimeInfo != null) {
						if (logger.isLoggable(Level.FINE)) {
							List<?> numOfEntries = runtimeInfo.m_NumOFEntries;
							for (int c = 0; c < runtimeInfo.m_ClassNames.size(); c++) 
								logger.fine(runtimeInfo.m_ClassNames.get(c) + " has " + numOfEntries.get(c).toString() + " objects"); 
						}
					}
					else {
						classNames.remove(className);
						logger.warning("space class export task - class: " + className + " was not found!");
					}
				}
				logger.info("confirmed " + classNames.size() + " classes");
				audit.add("confirmed " + classNames.size() + " classes");
			}
			else {
				Object classList[] = remoteAdmin.getRuntimeInfo().m_ClassNames.toArray();
				if (logger.isLoggable(Level.FINE)) {
					List<?> numOfEntries = remoteAdmin.getRuntimeInfo().m_NumOFEntries;
					for (int c = 0; c < classList.length; c++) 
						logger.fine(classList[c] + " has " + numOfEntries.get(c).toString() + " objects"); 
				}
				for (Object clazz : classList) {
					logger.fine(clazz.toString());
					if (! clazz.toString().equals(OBJECT)) classNames.add(clazz.toString());
				}
				logger.info("found " + classNames.size() + " classes");
				audit.add("found " + classNames.size() + " classes");
			}
			
			if (classNames.size() > 0)
				writeObjects(classNames);
		}
		else {
			File[] files = new File(DOT).listFiles(new ImportClassFileFilter(clusterInfo.getInstanceId()));
			List<String> fileNames = new ArrayList<String>();
			for (File file : files) {
				if (! classNames.isEmpty()) {
					// remove elements from the file list
					if (classNames.contains(getClassNameFromImportFile(file))) 
						fileNames.add(file.toString());
				}
				else fileNames.add(file.toString());
			}

			logger.info("importer found " + fileNames.size() + " files");
			audit.add("importer found " + fileNames.size() + " files");
			if (fileNames.size() > 0)
				readObjects(fileNames);
		}
		return audit;
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
			if (arg.getException() == null) {
				if (arg.getResult() != null) {
					for (String str : arg.getResult()) 
						result.add(str);
				}
			}
			else {
				result.add("exception:" + arg.getException().getMessage());
				arg.getException().printStackTrace();
			}
		}
		
		return result;
	}

	private void writeObjects(List<String> classList) {
		
		writeObjects(gigaSpace, clusterInfo, classList);
	}

	private void writeObjects(GigaSpace space, ClusterInfo clusterInfo, List<String> classList) {
		
		List<SpaceClassExportThread> threadList = new ArrayList<SpaceClassExportThread>();
		Integer partitionId = clusterInfo.getInstanceId();
		for (String className : classList) {
			File file = new File(className + DOT + partitionId + SUFFIX);
			SpaceClassExportThread operation = new SpaceClassExportThread(space, file, className, batch);
			logger.info("starting export thread for " + className);
			audit.add("starting export thread for " + className);
			threadList.add(operation);
			operation.start();
		}

		boolean terminated = false;
		if (threadList.size() > 0)
			logger.info("waiting for " + classList.size() + " export operations to complete");
		while (! terminated) {
			boolean running = false;
			for (SpaceClassExportThread thread : threadList)
				running |= ! thread.getState().equals(Thread.State.TERMINATED);
			if (! running) terminated = true;
			else {
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException e) {
					logger.warning("exception during thread sleep - " + e.getMessage());
				}
			}
		}
		for (SpaceClassExportThread thread : threadList) {
			for (String line : thread.getMessage())
				((List<String>) audit).add(line);
		}
		
		audit.add("finished writing " + classList.size() + " classes");
		logger.info("finished writing " + classList.size() + " classes");
		
	}

	private void readObjects(List<String> classList) {
		
		readObjects(gigaSpace, clusterInfo, classList);
	}
	
	private void readObjects(GigaSpace space, ClusterInfo clusterInfo, List<String> classList) {
		
		List<SpaceClassImportThread> threadList = new ArrayList<SpaceClassImportThread>();

		Integer partitionId = clusterInfo.getInstanceId();
		for (String className : classList) {
			// we're being passed a file instead of a class name
			File file = new File(className);
			logger.info("importing class " + getClassNameFromImportFile(file) + " into partition " + partitionId);
			audit.add("importing class " + getClassNameFromImportFile(file) + " into partition " + partitionId);
			SpaceClassImportThread operation = new SpaceClassImportThread(space, file, 1000);
			threadList.add(operation);
			operation.start();
		}
		boolean terminated = false;
		logger.info("waiting for " + classList.size() + " import operations to complete");
		while (! terminated) {
			boolean running = false;
			for (Thread thread : threadList) {
				running |= ! thread.getState().equals(Thread.State.TERMINATED);
			}
			if (! running) terminated = true;
			else {
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		for (SpaceClassImportThread thread : threadList) {
			for (String line : thread.getMessage()) {
				audit.add(line, false);
			}
		}
		logger.info("finished reading " + classList.size() + " files");
		audit.add("finished reading " + classList.size() + " files");
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
	


	@Override
	public void setClusterInfo(ClusterInfo clusterInfo) {
		this.clusterInfo = clusterInfo;
	}

}
