package mapreducer;

import java.util.concurrent.SynchronousQueue;

public class GlobalMessageQueue
{
    private SynchronousQueue<PeerNodeMessageType>[] msgQueue;

    public GlobalMessageQueue(int numQueues)
    {
        // I'm thinking of have one queue for each peer node
        // ## TODO:
        msgQueue = (SynchronousQueue<PeerNodeMessageType>[]) new SynchronousQueue[numQueues];

    }

    /**
     * Retrieve a message for a specific peer node
     * 
     * @param nodeID
     * @return
     */
    public PeerNodeMessageType GetNextMsg(int nodeID)
    {
        return msgQueue[nodeID].poll();
    }

    /**
     * Send a message to a specific peer node
     * 
     * @param destNodeID
     * @param msg
     */
    public void SendMsg(int destNodeID, PeerNodeMessageType msg)
    {
        msgQueue[destNodeID].add(msg);
    }

    /**
     * Sends a message to all peer nodes
     * 
     * @param msg
     */
    public void BroadcastMessage(PeerNodeMessageType msg)
    {
        for (int i = 0; i < msgQueue.length; i++)
        {
            msgQueue[i].add(msg);
        }
    }
}
