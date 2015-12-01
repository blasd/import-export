package com.gigaspaces.tools.importexport;

import com.beust.jcommander.JCommander;
import com.gigaspaces.async.AsyncFuture;
import com.gigaspaces.tools.importexport.config.ExportConfiguration;
import com.gigaspaces.tools.importexport.config.Operation;
import com.gigaspaces.tools.importexport.config.SpaceConnectionFactory;
import com.gigaspaces.tools.importexport.remoting.FileExportTask;
import com.gigaspaces.tools.importexport.remoting.ImportFilesTask;
import com.gigaspaces.tools.importexport.remoting.RemoteTaskResult;
import com.gigaspaces.tools.importexport.threading.ThreadAudit;
import com.google.common.base.Joiner;
import com.j_spaces.core.client.SpaceURL;
import org.openspaces.core.GigaSpace;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import static java.util.Collections.max;

public class Program {
    private static final String SYSTEM_LOGGER_KEY = "import-export";
    private static Logger logger = Logger.getLogger(SYSTEM_LOGGER_KEY);

	public static void main(String[] args) {
        Program p = new Program();
        p.run(parseArguments(args));
	}

    private void run(ExportConfiguration config) {
        SpaceConnectionFactory spaceConnectionFactory = new SpaceConnectionFactory(config);

        try {
            GigaSpace space = spaceConnectionFactory.space();
            Collection<Integer> partitions = calculatePartitionCount(space, config);

            logger.info(String.format("%s the following partitions: %s", config.getOperation() == Operation.EXPORT ? "Exporting" : "Importing", Joiner.on(",").join(partitions)));


            List<AsyncFuture<RemoteTaskResult>> tasks = new ArrayList<>();

            Integer newPartitionCount;

            if(config.getNewPartitionCount() != null && config.getNewPartitionCount() > 0)
                newPartitionCount = config.getNewPartitionCount();
            else
                newPartitionCount = max(partitions);

            if(config.getOperation() == Operation.EXPORT)
                logger.info(String.format("New partition count: %s", newPartitionCount));

            for(Integer partition : partitions){
                AsyncFuture<RemoteTaskResult> resultSet;

                if(config.getOperation() == Operation.IMPORT){
                    resultSet = space.execute(new ImportFilesTask(newPartitionCount, config), partition);
                } else if (config.getOperation() == Operation.EXPORT){
                    resultSet = space.execute(new FileExportTask(newPartitionCount, config), partition);
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
                    Thread.sleep(1000);
                }
            }

            spaceConnectionFactory.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Collection<Integer> calculatePartitionCount(GigaSpace space, ExportConfiguration config) {
        Collection<Integer> output = new ArrayList<>();

        if(config.getPartitions() != null && !config.getPartitions().isEmpty()){
            output.addAll(config.getPartitions());
        } else {
            SpaceURL url = space.getSpace().getURL();
            String total_members = url.getProperty("total_members");

            String[] split = total_members.split(",");
            int partitionCount = Integer.parseInt(split[0]);

            for(int x = 0; x < partitionCount; x++){
                output.add(x + 1);
            }
        }

        return output;
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
