package com.gigaspaces.tools.importexport.exception;

public class ImportExportException extends RuntimeException{

    public ImportExportException() {
    }

    public ImportExportException(String message) {
        super(message);
    }

    public ImportExportException(String message, Throwable cause) {
        super(message, cause);
    }

    public ImportExportException(Throwable cause) {
        super(cause);
    }

    public ImportExportException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
