package com.gigaspaces.tools.importexport;

import com.gigaspaces.logger.Constants;
import com.gigaspaces.tools.importexport.serial.SerialAudit;
import org.openspaces.core.GigaSpace;

import java.io.File;

public class AbstractSpaceThread {

    protected final static java.util.logging.Logger logger = java.util.logging.Logger.getLogger(Constants.LOGGER_COMMON);

    protected GigaSpace space;
    protected File file;
    protected Integer batch = 1000;
    protected SerialAudit lines;

    public SerialAudit getMessage() {
        return lines;
    }

    protected void logInfoMessage(String message){
        logger.info(message);
        lines.add(message);
    }


    protected void logFineMessage(String message){
        logger.fine(message);
        lines.add(message);
    }

}
