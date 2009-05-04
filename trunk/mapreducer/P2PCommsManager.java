package mapreducer;

public class P2PCommsManager implements Runnable
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
                // Read message queue
                PeerNodeMessageType msg = msgQueue.GetNextMsg(nodeID);

                mrHandler.ProcessPeerNodeMessage(msg);
            }

            try
            {
                Thread.sleep(1000);
            }
            catch (InterruptedException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
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
