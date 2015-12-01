package com.gigaspaces.tools.importexport;

import com.beust.jcommander.JCommander;
import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.tools.importexport.config.ExportConfiguration;
import com.gigaspaces.tools.importexport.config.Operation;
import com.gigaspaces.tools.importexport.config.SpaceConnectionFactory;
import com.gigaspaces.tools.importexport.remoting.FileExportTask;
import com.gigaspaces.tools.importexport.remoting.ImportFilesTask;
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
    private static final String SYSTEM_LOGGER_KEY = "import-export";
    private static Logger logger = Logger.getLogger(SYSTEM_LOGGER_KEY);

	public static void main(String[] args) {
        Program p = new Program();
        p.run(parseArguments(args));
	}

    private void run(ExportConfiguration config) {
        SpaceConnectionFactory spaceConnectionFactory = new SpaceConnectionFactory(config);
        RouteTranslator router = spaceConnectionFactory.createRouter();

        try {
            GigaSpace space = spaceConnectionFactory.createProxy();
            List<AsyncFuture<RemoteTaskResult>> tasks = new ArrayList<>();

            for(Integer partition : router.getTargetPartitions()){
                AsyncFuture<RemoteTaskResult> resultSet;

                if(config.getOperation() == Operation.IMPORT){
                    resultSet = space.execute(new ImportFilesTask(router.getDesiredPartitionCount(), config), partition);
                } else if (config.getOperation() == Operation.EXPORT){
                    resultSet = space.execute(new FileExportTask(router.getDesiredPartitionCount(), config), partition);
                } else {
                    throw new IllegalStateException("Invalid operation.");
                }

                tasks.add(resultSet);
            }

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

            spaceConnectionFactory.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void onResult(RemoteTaskResult result) {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("Partition %s Finished ---------------------\n\n", result.getPartitionId()));
        builder.append(String.format("\tPartition Id: %s\n", result.getPartitionId()));
        builder.append(String.format("\tProcess Id: %s\n", result.getProcessId()));
        builder.append(String.format("\tHostname: %s\n", result.getHostName()));

        if(result.getAudits() != null && !result.getAudits().isEmpty()) {
            builder.append("\n\tFiles:\n");

            for (ThreadAudit audit : result.getAudits()) {

                Exception threadException = audit.getException();

                if(audit.getCount() > 0 && threadException == null)
                    builder.append(String.format("\t%s | Records: %s\n", audit.getFileName(), audit.getCount()));
                else if(threadException != null){
                    builder.append(String.format("\t%s | Encountered exception.", audit.getFileName()));

                    StringWriter stringWriter = new StringWriter();
                    PrintWriter printWriter = new PrintWriter(stringWriter);
                    threadException.printStackTrace(printWriter);
                    builder.append("\n-- START OF EXCEPTION --\n");
                    builder.append(stringWriter.toString());
                    builder.append("-- END OF EXCEPTION --\n");
                    printWriter.close();
                }
            }
        }

        formatForException(builder, result.getException());
        logger.info(builder.toString());
    }

    private void formatForException(StringBuilder builder, Exception exception) {
        if(exception == null) return;

        builder.append("Exception encountered: \n");
        builder.append(exception.getMessage()).append("\n");

        for(StackTraceElement element : exception.getStackTrace()){
            builder.append(element.toString()).append("\n");
        }

    }

    public static ExportConfiguration parseArguments(String[] args) {
        ExportConfiguration output = new ExportConfiguration();
        JCommander jCommander = new JCommander(output);
        jCommander.parse(args);

        return output;
    }
}
