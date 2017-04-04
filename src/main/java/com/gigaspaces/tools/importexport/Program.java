package com.gigaspaces.tools.importexport;

import com.beust.jcommander.JCommander;
import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.tools.importexport.config.ExportConfiguration;
import com.gigaspaces.tools.importexport.config.Operation;
import com.gigaspaces.tools.importexport.config.SpaceConnectionFactory;
import com.gigaspaces.tools.importexport.remoting.FileExportTask;
import com.gigaspaces.tools.importexport.remoting.FileImportTask;
import com.gigaspaces.tools.importexport.remoting.RemoteTaskResult;
import com.gigaspaces.tools.importexport.remoting.RouteTranslator;
import com.gigaspaces.tools.importexport.threading.ThreadAudit;
import org.openspaces.core.GigaSpace;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class Program {
    private static Logger logger = Logger.getLogger(Constants.LOGGER_NAME);

	public static void main(String[] args) {
        Program p = new Program();
        p.run(parseArguments(args));
	}

    private void run(ExportConfiguration config) {
        SpaceConnectionFactory spaceConnectionFactory = new SpaceConnectionFactory(config);
        RouteTranslator router = spaceConnectionFactory.createRouter();

        try {
            GigaSpace space = spaceConnectionFactory.createProxy();
            List<AsyncFuture<RemoteTaskResult>> tasks = new ArrayList<AsyncFuture<RemoteTaskResult>>();

            for(Integer partition : router.getTargetPartitions()){
                AsyncFuture<RemoteTaskResult> resultSet;

                if(config.getOperation() == Operation.IMPORT){
                    resultSet = space.execute(new FileImportTask(config), partition);
                } else if (config.getOperation() == Operation.EXPORT){
                    resultSet = space.execute(new FileExportTask(router.getDesiredPartitionCount(), config), partition);
                } else {
                    throw new IllegalStateException("Invalid operation.");
                }

                tasks.add(resultSet);
            }

            logger.info(String.format("Started import/export operation with the following configuration: \n%s", config.toString()));

            waitOnTasks(config, tasks);

            spaceConnectionFactory.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void waitOnTasks(ExportConfiguration config, List<AsyncFuture<RemoteTaskResult>> tasks) throws InterruptedException, java.util.concurrent.ExecutionException {
        while(!tasks.isEmpty()){
            AsyncFuture<RemoteTaskResult> currentTask = tasks.get(0);
            if(currentTask.isDone() || currentTask.isCancelled()){
                tasks.remove(0);

                if(currentTask.isDone()){
                    onResult(currentTask.get());
                } else if (currentTask.isCancelled()){
                    onResult(currentTask.get());
                }
            } else {
                Thread.sleep(config.getThreadSleepMilliseconds());
            }
        }
    }

    private void onResult(RemoteTaskResult result) {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("Partition %s Finished ---------------------\n\n", result.getPartitionId()));
        builder.append(String.format("\tPartition Id: %s\n", result.getPartitionId()));
        builder.append(String.format("\tProcess Id: %s\n", result.getProcessId()));
        builder.append(String.format("\tHostname: %s\n", result.getHostName()));
        builder.append(String.format("\tElapsed Process Time (ms): %s\n", result.getElapsedTime()));

        builder.append("\n\tFiles:\n");
        if(result.getAudits() != null && !result.getAudits().isEmpty()) {

            for (ThreadAudit audit : result.getAudits()) {

                Exception threadException = audit.getException();

                if(audit.getCount() > 0 && threadException == null)
                    builder.append(String.format("\t\t%s | Records: %s | Elapsed time (ms): %s\n", audit.getFileName(), audit.getCount(), audit.getTime()));
                else if(threadException != null){
                    builder.append(String.format("\t\t%s | Encountered exception.", audit.getFileName()));
                    builder.append(formatException(threadException));
                }
            }
        }

        if(result.getExceptions() != null && !result.getExceptions().isEmpty()){
            builder.append("\n\tExceptions:");
            for(Exception ex : result.getExceptions()){
                builder.append(formatException(ex));
            }
        }

        logger.info(builder.toString());
    }

    private String formatException(Exception ex){
        StringBuilder output = new StringBuilder();
        output.append("\n\n-- EXCEPTION --\n");;
        StringWriter stringWriter = new StringWriter();
        PrintWriter printWriter = new PrintWriter(stringWriter);
        ex.printStackTrace(printWriter);
        printWriter.close();
        output.append(stringWriter.toString());
        output.append("\n---------------\n\n");

        return output.toString();
    }

    public static ExportConfiguration parseArguments(String[] args) {
        ExportConfiguration output = new ExportConfiguration();
        JCommander jCommander = new JCommander(output);
        jCommander.parse(args);

        return output;
    }
}
