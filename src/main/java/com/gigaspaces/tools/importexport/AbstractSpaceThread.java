package com.gigaspaces.tools.importexport;

import com.gigaspaces.logger.Constants;
import com.gigaspaces.tools.importexport.serial.Audit;
import org.openspaces.core.GigaSpace;

import java.io.*;
import java.util.concurrent.Callable;


public abstract class AbstractSpaceThread implements Callable<Audit> {

    protected final static java.util.logging.Logger logger = java.util.logging.Logger.getLogger(Constants.LOGGER_COMMON);

    protected GigaSpace space;
    protected File file;
    protected Integer batch = 1000;
    protected Audit result = new Audit();

    protected void logInfoMessage(String message){
        logger.info(message);
        result.add(message);
        System.out.println("!!!!!!!!!!!!!!!! " + message);
    }
    protected void logFineMessage(String message){
        logger.fine(message);
        result.add(message);
    }

    @Override
    public Audit call() throws Exception {
        return performOperation();
    }

    protected abstract Audit performOperation() throws Exception;
}
