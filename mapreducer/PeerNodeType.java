package mapreducer;

public class PeerNodeType
{
    public P2PCommsManager p2pComms;
    public MRProtocolHandler mrHandler;
    public Master master;
    public Worker worker;
    public JobClient jobClient;
    public FaultAndHealth faultHealth;
    private PeerNodeRoleType roleType;
    private int nodeID;
    
    public static int new_nodeID = 1;

    public PeerNodeType()
    {
        // Instantiate our 3 types of nodes which we could be
        master = new Master();
        worker = new Worker();
        jobClient = new JobClient();
        
        faultHealth = new FaultAndHealth();

        // Instantiate the comms and fault modules
        p2pComms = new P2PCommsManager(new_nodeID);

        // Set node ID
        master.nodeID = new_nodeID;
        worker.nodeID = new_nodeID;
        jobClient.nodeID = new_nodeID;
        
        // set a master node ID for reference from the simulator
        nodeID = new_nodeID;
        
        // Increment new node ID
        new_nodeID = new_nodeID + 1;

        // Instantiate and configure the MRProtocolHandler
        mrHandler = new MRProtocolHandler();
        mrHandler.SetNodeType(roleType);
        mrHandler.SetFaultAndHealthReference(faultHealth);
        mrHandler.SetJobClientReference(jobClient);
        mrHandler.SetMasterReference(master);
        mrHandler.SetP2PCommsManagerReference(p2pComms);
        mrHandler.SetWorkerReference(worker);

        p2pComms.SetMRProtocolHandlerRef(mrHandler);

        // Set master reference to the mrHandler
        master.SetMRProtocolHandlerRef(mrHandler);

        // Set job client reference to the mrHandler
        jobClient.SetMRProtocolHandlerRef(mrHandler);

        // Set worker reference to the mrHandler
        worker.SetMRProtocolHandlerRef(mrHandler);
        
        faultHealth.SetMRProtocolHandlerRef(mrHandler);
        faultHealth.SetP2PCommsManagerReference(p2pComms);


        // TODO: Still need references in FaultAndHealth ????
    }

    public void setNodeName()
    {
        mrHandler.SetNodeName();
    }

    public void setRoleType(PeerNodeRoleType role)
    {
        roleType = role;
        mrHandler.SetNodeType(role);
    }

    public PeerNodeRoleType getRoleType()
    {
        return roleType;
    }
    
    public int getNodeID()
    {
    	return nodeID;
    }

    /**
     * Starts the appropriate threads for this node
     */
    public void run()
    {
        // Start the Communcations Thread
        p2pComms.start();

        // Based on our assigned role, we will start the appropriate node type

        if (roleType == PeerNodeRoleType.CLIENT)
        {
            jobClient.start();
        }
        else if (roleType == PeerNodeRoleType.MASTER)
        {
        	//put this in to get some debug info to show up
            master.start();
        }
        else if (roleType == PeerNodeRoleType.WORKER)
        {
            worker.start();
        }
    }

    public void sim_InitialMasterNodeIDBroadcast()
    {
        mrHandler.BroadcastNewMasterNode();
    }
    
    public void sim_SetNewWorkerNode(int nodeID)
    {
    	mrHandler.sim_NewWorkerNodeConnected(nodeID);
    }
    
    public void sim_InitialBroadcastWorkerNodeList()
    {
    	mrHandler.BroadcastWorkerNodeList();
    }
}
