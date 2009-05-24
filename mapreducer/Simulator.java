package mapreducer;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;

public class Simulator implements Runnable
{
    public EventLogging eventLogger;
    public StatisticsLogging statLogger;
    public ConfigSettings config;
    public Hashtable<Integer, PeerNodeType> peerNodes;
    
    public PeerNodeType masterNode;

    public Simulator() throws IOException
    {
    	eventLogger = new EventLogging();		//initialize logger class
    }

    public void run()
    {
        // Initialize and get the simulator up and running
        initializeMapReduceSimulator();

        // Now run the scenarios ??
        
        
        //BS TEST BEGIN  - Uncomment to fail a worker node and redistribute his work to another worker
/*        
        int workerNodeID = getWorkerToFail();
        PeerNodeType node = peerNodes.get(workerNodeID);
        node.mrHandler.commsMgr.sim_FailThisNode();
*/
        //BS TEST END

    }
    
    private int getWorkerToFail()
    {
    	//Select the first worker that we find for failure
    	
    	int ret = 0;
    	
    	Iterator<Integer> iter = peerNodes.keySet().iterator();
    	
    	while(iter.hasNext())
    	{
    		int nodeID = iter.next();
    		
    		if (peerNodes.get(nodeID).getRoleType() == PeerNodeRoleType.WORKER)
    		{
    			ret = nodeID;
    			break;
    		}
    	}
    	
    	return ret;
    }

    private void initializeMapReduceSimulator()
    {
        peerNodes = new Hashtable<Integer, PeerNodeType>();

        // Create all nodes and add to hashtable
        // Initially set all nodes to workers
        for (int i = 0; i < ConfigSettings.numNodes; i++)
        {
            int id;

            PeerNodeType node = new PeerNodeType();
            id = node.getNodeID();
            node.setRoleType(PeerNodeRoleType.WORKER);
            node.setNodeName();
            peerNodes.put(id, node);
        }

        // Randomly select initial job client
        selectRandomJobClient();

        // Randomly select initial job client
        selectRandomMaster();

        // Next, we need to notify the Worker and JobClient nodes who the master
        // node is. We need to do this now because we need to make sure all
        // nodes
        // know who the master is during the startup of the simulator. Later on,
        // this logic will be used when the Master Election logic is run and a
        // new
        // Master is elected.
        // Find the master node.
        masterNode.sim_InitialMasterNodeIDBroadcast();
        
        // Need to propagate the worker node list
        Enumeration<PeerNodeType> we = peerNodes.elements();
        PeerNodeType workerNode;
        while (we.hasMoreElements())
        {
            workerNode = we.nextElement();
            
            if (workerNode.getRoleType() == PeerNodeRoleType.WORKER)
            {
               masterNode.sim_SetNewWorkerNode(workerNode.p2pComms.GetNodeID());
            }
        }        
        masterNode.sim_InitialBroadcastWorkerNodeList();

        // Run nodes
        // This will start the communications thread and the map/reduce thread
        // in the Worker Node and the JobClient thread which discovers the
        // master node
        runNodeThreads();
        
        // TODO: Are there other things to do here ???

    }

    /**
     * 
     */
    private void selectRandomJobClient()
    {
        int jobClientIndex = (int) (0 + Math.random() * ConfigSettings.numNodes);
        Integer jobClientKey = (Integer) peerNodes.keySet().toArray()[jobClientIndex];
        peerNodes.get(jobClientKey).setRoleType(PeerNodeRoleType.CLIENT);
        peerNodes.get(jobClientKey).setNodeName();
    }

    /**
     * 
     */
    private void selectRandomMaster()
    {
        boolean foundMaster = false;

        // Make sure that our initial master is not the client we just selected

        while (!foundMaster)
        {
            int masterClientIndex = (int) (0 + Math.random() * ConfigSettings.numNodes);
            Integer masterClientKey = (Integer) peerNodes.keySet().toArray()[masterClientIndex];

            if (peerNodes.get(masterClientKey).getRoleType() != PeerNodeRoleType.CLIENT)
            {
                peerNodes.get(masterClientKey).setRoleType(PeerNodeRoleType.MASTER);
                peerNodes.get(masterClientKey).setNodeName();
                masterNode = peerNodes.get(masterClientKey);
                foundMaster = true;
            }
        }
    }

    /**
     * 
     */
    private void runNodeThreads()
    {
        // Run all node threads
        Enumeration<PeerNodeType> e = peerNodes.elements();

        while (e.hasMoreElements())
            e.nextElement().run();
    }
}