package mapreducer;

public class Master extends Thread
{
    public MRProtocolHandler mrHandler;

    public Master()
    {

    }

    public void SetMRProtocolHandlerRef(MRProtocolHandler reference)
    {
        mrHandler = reference;
    	mrHandler.SetNodeType(PeerNodeRoleType.MASTER);
    }
    
	//Method that must be implemented to run worker as a thread
	public void run() 
	{	
		//Set reference that we're the master
		mrHandler.SetMasterReference(this);
		mrHandler.UpdateMasterNode(this.hashCode());
		
		System.out.println("SET MASTER");
		
		while(true)
		{
			//System.out.println("Master Thread:  " + this.hashCode() + " is running");
		}
	}
	
	// Method to handle PeerNodeMessageType.SUBMIT_MR_JOB
	public void workerSubmittedJob()
	{
		// Worker has submitted the job, now the master must divide up the work
		// TODO Put algorithm here to decide how to divide up work
	}
	
	// Method to handle PeerNodeMessageType.MR_JOB_COMPLETE
	public void jobComplete()
	{
		// TODO Tell the job client the work is complete
	}
}
