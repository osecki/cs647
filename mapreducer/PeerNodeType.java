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
    
    
    public PeerNodeType()
    {
    	//Instantiate our 3 types of nodes which we could be
    	master = new Master();
    	worker = new Worker();
    	jobClient = new JobClient();
    	
    	//Instantiate the comms and fault modules
    	p2pComms = new P2PCommsManager(this.hashCode());
    	faultHealth = new FaultAndHealth();
    
    	//Instantiate and configure the MRProtocolHandler
    	mrHandler = new MRProtocolHandler();
    	mrHandler.SetNodeType(roleType);
    	mrHandler.SetFaultAndHealthReference(faultHealth);
    	mrHandler.SetJobClientReference(jobClient);
    	mrHandler.SetMasterReference(master);
    	mrHandler.SetP2PCommsManagerReference(p2pComms);
    	mrHandler.SetWorkerReference(worker);
    	
    	//Set job client reference to the mrHandler
    	jobClient.SetMRProtocolHandlerRef(mrHandler);
    }
    
    public void setRoleType(PeerNodeRoleType role)
    {
    	roleType = role;
    }

    public PeerNodeRoleType getRoleType()
    {
    	return roleType;
    }
    
    public void run()
    {	
    	//Based on our assigned role, we will start the appropriate node type
    	
    	if (roleType == PeerNodeRoleType.CLIENT)
    	{
    		jobClient.start();
    	}
    	else if (roleType == PeerNodeRoleType.MASTER)
    	{    		
    		master.start();
    	}
    	else if (roleType == PeerNodeRoleType.WORKER)
    	{
    		worker.start();
    	}
    }
}