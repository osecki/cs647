package mapreducer;

import java.io.IOException;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.SimpleLayout;

public class EventLogging 
{
	static Logger logger = Logger.getLogger(EventLogging.class);
	
	public EventLogging () throws IOException
	{
		BasicConfigurator.configure();
	
		SimpleLayout layout = new SimpleLayout();
		FileAppender appender = new FileAppender(layout, ConfigSettings.eventLogPath, false);
	    logger.addAppender(appender);
	    
	    if (ConfigSettings.eventLogLevel.equals("debug"))    
	    	logger.setLevel((Level) Level.DEBUG);
	    if (ConfigSettings.eventLogLevel.equals("info"))    
	    	logger.setLevel((Level) Level.INFO);
	    if (ConfigSettings.eventLogLevel.equals("error"))    
	    	logger.setLevel((Level) Level.ERROR);
	    if (ConfigSettings.eventLogLevel.equals("warn"))    
	    	logger.setLevel((Level) Level.WARN);
	    if (ConfigSettings.eventLogLevel.equals("fatal"))    
	    	logger.setLevel((Level) Level.FATAL);
	}
	
	public static void debug(String message)
	{
		logger.debug(message);
	}
	
	public static void info(String message)
	{
		logger.info(message);
	}
	
	public static void warn(String message)
	{
		logger.warn(message);
	}
	
	public static void error(String message)
	{
		logger.error(message);
	}
	
	public static void fatal(String message)
	{
		logger.fatal(message);
	}
}
