package mapreducer;


public class P2PCommsManager implements Runnable
{
	public GlobalMessageQueue msgQueue;
    public MRProtocolHandler mrHandler;
    
    private int nodeID;    
    
    /**
     * 
     * @param id
     */
    public P2PCommsManager(int id)
    {
        nodeID = id;	
    }
    
    /**
     * 
     * @param reference
     */
    public void SetGlobalMessageQueueRef(GlobalMessageQueue reference)
    {
    	msgQueue = reference;
    }
    
    /**
     * 
     * @param reference
     */
    public void SetMRProtocolHandlerRef(MRProtocolHandler reference)
    {
    	mrHandler = reference;
    }
    
    public int GetNodeID()
    {
    	return nodeID;
    }
    
    /**
     * 
     */
    public void run()
    {
    	boolean runLoop = true;
    	
    	while(runLoop)
    	{
    		// Read message queue
    	}
    }
}
