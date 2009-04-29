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
    	master = new Master();
    	worker = new Worker();
    	jobClient = new JobClient();
    	
    	p2pComms = new P2PCommsManager(this.hashCode());
    	faultHealth = new FaultAndHealth();
    	
    	
    	//Configure the MRProtocolHandler
    	mrHandler = new MRProtocolHandler();
    	mrHandler.SetNodeType(roleType);
    	mrHandler.SetFaultAndHealthReference(faultHealth);
    	mrHandler.SetJobClientReference(jobClient);
    	mrHandler.SetMasterReference(master);
    	mrHandler.SetP2PCommsManagerReference(p2pComms);
    	mrHandler.SetWorkerReference(worker);
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
    	if (roleType == PeerNodeRoleType.CLIENT)
    	{
    		jobClient.start();
    	}
    	else if (roleType == PeerNodeRoleType.MASTER)
    	{
    		
    	}
    	else if (roleType == PeerNodeRoleType.WORKER)
    	{
    	}
    }
}
