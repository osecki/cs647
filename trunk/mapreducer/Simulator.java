package mapreducer;

import java.util.*;

public class Simulator implements Runnable
{
    public EventLogging eventLogger;
    public StatisticsLogging statLogger;
    public P2PCommsManager[] p2pComms;
    public MRProtocolHandler[] mrHandler;
    public Master[] master;
    public Hashtable<Integer, Worker> workerList;	//key = hashCode;  value = worker object
    public JobClient[] jobClient;
    public FaultAndHealth[] faultHealth;
    public ConfigSettings config;

    public Simulator()
    {
        // TODO: Instantiate everything, pass references, etc
    	workerList = new Hashtable<Integer, Worker>();

        // TODO: Start all threads
    	
    	
    	//Create all worker threads and add to hashtable
    	for (int i = 0; i < ConfigSettings.numWorkers; i++)
    	{
    		Worker workerNode = new Worker();
    		workerList.put(workerNode.hashCode(), workerNode);
    	}
    }

    public void run()
    {
    	runWorkerThreads();
    }
    
    private void runWorkerThreads()
    {
    	//Run all worker threads
    	Enumeration<Worker> e = workerList.elements();
    	
    	while (e.hasMoreElements())
    		e.nextElement().start();    	
    }

}
