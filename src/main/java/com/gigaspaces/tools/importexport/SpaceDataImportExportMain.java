package com.gigaspaces.tools.importexport;

import java.util.List;
import java.util.logging.Logger;

import com.beust.jcommander.JCommander;
import com.gigaspaces.tools.importexport.config.InputParameters;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.UrlSpaceConfigurer;

import com.gigaspaces.async.AsyncFuture;

import static com.google.common.base.Strings.*;

/**
 * @author jb
 *
 */
public class SpaceDataImportExportMain {

	private static Logger logger = Logger.getLogger("com.gigaspaces.common");

    private InputParameters params;

	private GigaSpace space;

	public void init(String[] args) {
        InputParameters params1 = new InputParameters();
        JCommander jCommander = new JCommander(params1);
        jCommander.parse(args);
        params = params1;
	}
	
	public void verify() {
		if (! params.getExport() && ! params.getImp()) {
			System.out.println("operation required: specify export or import");
			showHelpMessage();
		}
		// perform other steps as neccessary
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		SpaceDataImportExportMain exporter = new SpaceDataImportExportMain();

		exporter.init(args);
		exporter.verify();
        InputParameters config = exporter.params;
		try {
			UrlSpaceConfigurer urlConfig = new UrlSpaceConfigurer(new SpaceURLBuilder(config).buildSpaceURL());

            if(!isNullOrEmpty(config.getUsername())){
                urlConfig.credentials(config.getUsername(), config.getPassword());
            }

			GigaSpaceConfigurer gigaConfig = new GigaSpaceConfigurer(urlConfig.lookupTimeout(20000).space());

			exporter.setSpace(gigaConfig.gigaSpace());

			ExportImportTask task = new ExportImportTask(config);

			AsyncFuture<List<String>> results = null;
			if (config.getPartitions().isEmpty()) {
				results = exporter.getSpace().execute(task); 
			}
			else {
				Object[] spacePartitions = config.getPartitions().toArray(new Integer[0]);
				results = exporter.getSpace().execute(task, spacePartitions); 
			}
			System.out.print("executing tasks");
			while (! results.isDone()) {
				Thread.sleep(1000L);
				System.out.print(".");
			}
			System.out.println("");
			
			// report the results here
			for (String result : results.get()) {
//				logger.info((exporter.getExport() ? "exporter " : " importer ") + result);
				logger.info(result);
			}
			
			urlConfig.destroy();
		} 
		catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void showHelpMessage() {
		//TODO
	}

	public void setSpace(GigaSpace space) {
		this.space = space; 
	}
	
	public GigaSpace getSpace() {
		return space;
	}
}
