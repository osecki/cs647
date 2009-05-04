package mapreducer;

import mapreducer.ConfigSettings;
import mapreducer.Master.*;
import mapreducer.Simulator.*;

public class Main 
{
    private static EventLogging eventLogger;
    private static StatisticsLogging statLogger;
    private static Master master;
    private static Simulator sim;
    
    public static void main(String[] args) 
	{
		ConfigSettings.Init(); // Read in configuration file settings
		
		eventLogger = new EventLogging();
		statLogger = new StatisticsLogging();
		master = new Master();
	    sim = new Simulator();
	    
	    //sim.SetMasterRef(master);
	    
	    sim.run();
	}
}
