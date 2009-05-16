package mapreducer;

import java.io.IOException;

import mapreducer.ConfigSettings;
import mapreducer.Master.*;
import mapreducer.Simulator.*;

public class Main 
{
    private static StatisticsLogging statLogger;
    private static Simulator sim;
    
    public static void main(String[] args) throws IOException 
	{
		ConfigSettings.Init(); // Read in configuration file settings
		
		statLogger = new StatisticsLogging();
	    sim = new Simulator();
	    
	    //sim.SetMasterRef(master);
	    
	    sim.run();
	}
}
