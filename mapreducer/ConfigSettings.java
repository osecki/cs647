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
    public static String eventLogPath;
    public static String eventLogLevel;
    public static String scenarioToRun;
    public static int msgPollInterval;
    public static int heartbeatInterval;
    public static int masterNodeFailureTimeout;
    public static int workerNodeFailureTimeout;
    public static boolean failWorkerNode;
    public static long failWorkerNodeDelay;
    public static boolean failMasterNode;
    public static long failMasterNodeDelay;

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

                eventLogPath = pro.getProperty("eventLoggerFile");

                eventLogLevel = pro.getProperty("eventLoggerLevel");

                scenarioToRun = pro.getProperty("scenarioToRun");
                
                msgPollInterval = Integer.parseInt(pro.getProperty("msgPollInterval"));
                
                heartbeatInterval = Integer.parseInt(pro.getProperty("heartbeatInterval"));
                
                masterNodeFailureTimeout = Integer.parseInt(pro.getProperty("masterNodeFailureTimeOut"));
                
                workerNodeFailureTimeout = Integer.parseInt(pro.getProperty("workerNodeFailureTimeOut"));
                
                failWorkerNode = Boolean.parseBoolean(pro.getProperty("failworkernode"));
                
                failWorkerNodeDelay = Long.parseLong(pro.getProperty("failworkernodedelay"));
                
                failMasterNode = Boolean.parseBoolean(pro.getProperty("failmasternode"));
                
                failMasterNodeDelay = Long.parseLong(pro.getProperty("failmasternodedelay"));
                
            }
        }
        catch (IOException e)
        {
            System.out.println(e.getMessage());
        }
    }
}
