package mapreducer;

public class Worker extends Thread
{
    /*
     * TODO THE BIG QUESTION...Do we want to run the worker as another thread so
     * that the message queue could still be read and more jobs be scheduled for
     * the worker while it is working on a job
     */

    public MRProtocolHandler mrHandler;

    public Worker()
    {
    }

    public void SetMRProtocolHandlerRef(MRProtocolHandler reference)
    {
        mrHandler = reference;
    }

	//Method that must be implemented to run worker as a thread
	public void run() 
	{	
		//Tell simulator that we are connected
		mrHandler.sim_NewWorkerNodeConnected(this.hashCode());
		
		while(true)
		{
			//System.out.println("Worker Thread:  " + this.hashCode() + " is running");
		}
	}
}
