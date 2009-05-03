package mapreducer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class ConfigSettings
{
    public static int numNodes;
    public static String workTextFile;
    public static boolean selfHealing;
    public static boolean selfOptimization;

    public static void Init()
    {
        try
        {
            // Load the configuration file
            BufferedReader bf = new BufferedReader(new InputStreamReader(System.in));
            File f = new File("Config.properties");

            if (f.exists())
            {
                Properties pro = new Properties();
                FileInputStream in = new FileInputStream(f);
                pro.load(in);

                // load number of nodes to spawn
                numNodes = Integer.parseInt(pro.getProperty("numNodes"));

                workTextFile = pro.getProperty("workTextFile");
                
                // load flag to determine if we want to self heal
                selfHealing = Boolean.parseBoolean(pro.getProperty("selfHealing"));

                // load flag to determine if we want to self optimize
                selfOptimization = Boolean.parseBoolean(pro.getProperty("selfOptimization"));
            }
        }
        catch (IOException e)
        {
            System.out.println(e.getMessage());
        }
    }
}
