package mapreducer;

public class JobClient extends Thread
{
    public MRProtocolHandler mrHandler;

    public JobClient()
    {

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
