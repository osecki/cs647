import java.io.*;
import java.util.*;

public class ConfigSettings 
{
	public static int numWorkers;
	public static boolean selfHealing;
	public static boolean selfOptimization;
	
	public static void Init()
	{
		try
		{
			//Load the configuration file
			BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
	        File f = new File("Config.properties");
		        
	        if(f.exists())
		    {
	        	Properties pro = new Properties();
		        FileInputStream in = new FileInputStream(f);
		        pro.load(in);
		        
		        //load number of worker threads to spawn
		        numWorkers = Integer.parseInt(pro.getProperty("numWorkers"));
		        
		        //load flag to determine if we want to self heal
		        selfHealing = Boolean.parseBoolean(pro.getProperty("selfHealing"));

		        //load flag to determine if we want to self optimize
		        selfOptimization = Boolean.parseBoolean(pro.getProperty("selfOptimization"));
		    }
		}
	   catch(IOException e)
	   {
		   System.out.println(e.getMessage());
	   }
	}
}
