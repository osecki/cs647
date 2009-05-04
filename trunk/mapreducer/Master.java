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
	
	public void workerSubmittedJob()
	{
		//worker has submitted the job, now the master must 
		//divide up the work
	}
}
