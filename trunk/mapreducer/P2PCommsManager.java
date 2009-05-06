package mapreducer;

public class P2PCommsManager extends Thread
{
    public GlobalMessageQueue msgQueue;
    public MRProtocolHandler mrHandler;

    private boolean allowNodeToRun = true;

    private int nodeID;

    /**
     * @param id
     */
    public P2PCommsManager(int id)
    {
        nodeID = id;

        msgQueue = GlobalMessageQueue.GetInstance();
        msgQueue.CreateMessageQueue(id);
    }

    /**
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

    public void sim_FailThisNode()
    {
        allowNodeToRun = false;
    }

    public void sim_RestartThisNode()
    {
        allowNodeToRun = true;
    }

    /**
     * Run the thread
     */
    public void run()
    {
        boolean runLoop = true;

        while (runLoop)
        {
        	
            if (allowNodeToRun == true)
            {
            	try
            	{
                    // Read message queue
                    PeerNodeMessageType msg = msgQueue.GetNextMsg(nodeID);
                    
                    if (msg != null)
                    {      
                    	mrHandler.ProcessPeerNodeMessage(msg);
                    }
            	}
            	catch(Exception ex)
            	{
            		System.out.println("Exception in P2PCommMsg::Run " + ex.getMessage());
            	}
            }
        }
    }

    public void SendMsg(PeerNodeMessageType msg)
    {
        msg.sourceNode = nodeID;

        if (msg.destNode == PeerNodeMessageType.BROADCAST_DEST_ID)
        {
            msgQueue.BroadcastMessage(msg);
        }
        else
        {
            msgQueue.SendMsg(msg.destNode, msg);
        }
    }
}
