package mapreducer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.SimpleLayout;

public class EventLogging 
{
	static Logger logger = Logger.getLogger(EventLogging.class);
	static final String DATE_FORMAT_NOW = "yyyy-MM-dd HH:mm:ss";
	
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
	
	private static String now() 
	{
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
		return sdf.format(cal.getTime());
	}
	
	public static void debug(String message)
	{
		logger.debug(now() + " : " + message);
	}
	
	public static void info(String message)
	{
		logger.info(now() + " : " + message);
	}
	
	public static void warn(String message)
	{
		logger.warn(now() + " : " + message);
	}
	
	public static void error(String message)
	{
		logger.error(now() + " : " + message);
	}
	
	public static void fatal(String message)
	{
		logger.fatal(now() + " : " + message);
	}
}
