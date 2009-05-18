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

    /**
     * Method for Simulator to call that will simulate a failure of this node.
     * By skipping the polling of the message queue, this node will not respond
     * to the HeartBeat signal.
     */
    public void sim_FailThisNode()
    {
        allowNodeToRun = false;
    }

    /**
     * Method for Simulator to call that will restart this node.
     */
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

        String threadName = mrHandler.GetNodeName() + ":CommsMgr";
        //System.out.println("Starting Thread => " + threadName);

        this.setName(threadName);

        while (runLoop)
        {

            if (allowNodeToRun == true)
            {
                try
                {               	
                	
                    // Read message queue
                    PeerNodeMessageType msg = msgQueue.GetNextMsg(nodeID);
                    // System.out.println("Calling GetNextMsg" + ":" +
                    // mrHandler.GetNodeName());
                    
                    if (msg != null)
                    {
                        mrHandler.ProcessPeerNodeMessage(msg);
                    }
                    
                 // We are using the comms mgr loop to drive other activities that need to be done
                	// on a regular interval like heart beat sending(master node) and checking to see
                	// if master is still alive(worker node)
                	mrHandler.PerformHouseKeeping();

                    Thread.sleep(ConfigSettings.msgPollInterval);
                }
                catch (Exception ex)
                {
                    System.out.println(threadName + " Exception in P2PCommMsg::Run " + ex.getMessage());
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
