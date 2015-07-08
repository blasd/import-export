package com.gigaspaces.tools.importexport;

import com.gigaspaces.logger.Constants;
import com.gigaspaces.tools.importexport.serial.ThreadExecutionResult;
import org.openspaces.core.GigaSpace;

import java.io.*;
import java.util.concurrent.Callable;


public abstract class AbstractSpaceThread implements Callable<ThreadExecutionResult> {

    protected final static java.util.logging.Logger logger = java.util.logging.Logger.getLogger(Constants.LOGGER_COMMON);

    protected GigaSpace space;
    protected File file;
    protected Integer batch = 1000;
    protected ThreadExecutionResult result = new ThreadExecutionResult();

    protected void logInfoMessage(String message){
        logger.info(message);
        result.getAudit().add(message);
    }
    protected void logFineMessage(String message){
        logger.fine(message);
        result.getAudit().add(message);
    }

    @Override
    public ThreadExecutionResult call() throws Exception {
        return performOperation();
    }

    protected abstract ThreadExecutionResult performOperation() throws Exception;
}
