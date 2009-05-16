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
		FileAppender appender = new FileAppender(layout, ConfigSettings.eventLogPath, true);
	    logger.addAppender(appender);
	    logger.setLevel((Level) Level.DEBUG);
		
	      logger.debug("Here is some DEBUG");
	      logger.info("Here is some INFO");
	      logger.warn("Here is some WARN");
	      logger.error("Here is some ERROR");
	      logger.fatal("Here is some FATAL");

	}
	
}
