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
    public static String wordToSearch;

    public static void Init()
    {
        try
        {
            // Load the configuration file
            File f = new File("Config.properties");

            if (f.exists())
            {
                Properties pro = new Properties();
                FileInputStream in = new FileInputStream(f);
                pro.load(in);

                // load number of nodes to spawn
                numNodes = Integer.parseInt(pro.getProperty("numNodes"));

                workTextFile = pro.getProperty("workTextFile");

                wordToSearch = pro.getProperty("wordToSearch");

            }
        }
        catch (IOException e)
        {
            System.out.println(e.getMessage());
        }
    }
}
