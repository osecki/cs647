package mapreducer;

public class JobClient extends Thread
{
	//pass down mrHandler
	MRProtocolHandler mrHandler;

    public JobClient()
    {
    	//Set job client reference in handler
    	mrHandler.SetJobClientReference(this);
    }

    public void SetMRProtocolHandlerRef(MRProtocolHandler reference)
    {
        mrHandler = reference;
    }
    
    public void run()
    {
    	while(true)
    	{
    		System.out.println("Running Job Client");
    	}
    }

}
