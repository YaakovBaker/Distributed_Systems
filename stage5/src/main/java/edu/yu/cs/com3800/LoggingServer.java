package edu.yu.cs.com3800;


import java.io.File;
import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public interface LoggingServer {

    default Logger initializeLogging(String fileNamePreface) throws IOException {
        return initializeLogging(fileNamePreface,false);
    }
    default Logger initializeLogging(String fileNamePreface, boolean disableParentHandlers) throws IOException {
        return createLogger(fileNamePreface,fileNamePreface,disableParentHandlers);
    }

    static Logger createLogger(String loggerName, String fileNamePreface, boolean disableParentHandlers) throws IOException {
        Logger serverLogger = Logger.getLogger(loggerName);
        File newDIR = new File(".\\stage5Logs\\");
        if( !newDIR.exists() ){
            newDIR.mkdir();
        }
        FileHandler fh; 
        try {
            fh = new FileHandler(".\\stage5Logs\\" + fileNamePreface);
            fh.setLevel(Level.ALL);
            fh.setFormatter(new SimpleFormatter());
            serverLogger.addHandler(fh);
            serverLogger.setUseParentHandlers(disableParentHandlers);
        }catch (SecurityException e) {
            e.printStackTrace();
        }catch (IOException e) {
            e.printStackTrace();
        }
        return serverLogger;
    }
}
