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

    public PeerNodeMessageType GetNextMsg(int nodeID)
    {
        return msgQueue[nodeID].poll();
    }

    public void SendMsg(int destNodeID, PeerNodeMessageType msg)
    {
        msgQueue[destNodeID].add(msg);
    }

    public void BroadcastMessage(PeerNodeMessageType msg)
    {

    }
}
