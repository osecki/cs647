package mapreducer;

public class JobClient extends Thread
{
	//pass down mrHandler
	MRProtocolHandler mrHandler;

    public JobClient()
    {
    }

    public void SetMRProtocolHandlerRef(MRProtocolHandler reference)
    {
        mrHandler = reference;
    
    	//Set job client reference in handler
    	mrHandler.SetJobClientReference(this);
    }
    
    public void run()
    {
    	boolean masterExists = false;
    	String fileName = ConfigSettings.workTextFile;
    	
    	//Need to only submit job if a master exists
    	while(!masterExists)
    	{
    		
    		if (mrHandler.GetMasterNode() > 0)
    		{
    			System.out.println("FOUND MASTER!!! ");
    			masterExists = true;
    			mrHandler.SubmitMRJob(fileName);
    		}
    	}
    	
    	while(true)
    	{
    		//System.out.println("Job Client Thread:  " + this.hashCode() + " is running");
    	}
    }

}
