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
    	while(true)
    	{
    		System.out.println("Job Client Thread:  " + this.hashCode() + " is running");
    	}
    }

}
