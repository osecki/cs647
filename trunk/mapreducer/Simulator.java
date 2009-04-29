package mapreducer;

import java.util.Enumeration;
import java.util.Hashtable;

public class Simulator implements Runnable
{
    public EventLogging eventLogger;
    public StatisticsLogging statLogger;
    public ConfigSettings config;
    public Hashtable<Integer,PeerNodeType> peerNodes;

    public Simulator()
    {
        // TODO: Instantiate everything, pass references, etc
        peerNodes = new Hashtable<Integer, PeerNodeType>();

        // TODO: Start all threads

        // Create all nodes and add to hashtable
        // Initially set all nodes to workers
        for (int i = 0; i < ConfigSettings.numNodes; i++)
        {
            PeerNodeType node = new PeerNodeType();
            node.setRoleType(PeerNodeRoleType.WORKER);
            peerNodes.put(node.hashCode(), node);
        }
        
        //Randomly select initial job client
        selectRandomJobClient();

        //Randomly select initial job client
        selectRandomMaster();   
    }

    public void run()
    {
    	//Run nodes
        runNodeThreads();    
    }
    
    private void selectRandomJobClient()
    {
    	int jobClientIndex = (int) ( 0 + Math.random() * ConfigSettings.numNodes);
        Integer jobClientKey = (Integer)peerNodes.keySet().toArray()[jobClientIndex];
        peerNodes.get(jobClientKey).setRoleType(PeerNodeRoleType.CLIENT);
    }
    
    private void selectRandomMaster()
    {
    	boolean foundMaster = false;
    	
    	//Make sure that our initial master is not the client we just selected
    	
    	while(!foundMaster)
    	{
        	int masterClientIndex = (int) ( 0 + Math.random() * ConfigSettings.numNodes);
            Integer masterClientKey = (Integer)peerNodes.keySet().toArray()[masterClientIndex];
            
            if (peerNodes.get(masterClientKey).getRoleType() != PeerNodeRoleType.CLIENT)
            {
                peerNodes.get(masterClientKey).setRoleType(PeerNodeRoleType.MASTER);
                foundMaster = true;
            }
    	}	
    }

    private void runNodeThreads()
    {
        // Run all node threads
        Enumeration<PeerNodeType> e = peerNodes.elements();

        while (e.hasMoreElements())
            e.nextElement().run();       
    }
}
