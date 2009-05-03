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
		while(true)
		{
			System.out.println("Master Thread:  " + this.hashCode() + " is running");
		}
	}
}
