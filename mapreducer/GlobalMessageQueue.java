package mapreducer;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.SynchronousQueue;

public class GlobalMessageQueue
{
    private static GlobalMessageQueue instance = null;
    // private SynchronousQueue<PeerNodeMessageType> newMsgQueue;

    private Hashtable<Integer, LinkedList<PeerNodeMessageType>> messageQueues;

    public GlobalMessageQueue()
    {
    	messageQueues = new Hashtable<Integer, LinkedList<PeerNodeMessageType>>();
    }

    /**
     * Returns reference to the singleton
     * @return
     */
    public static GlobalMessageQueue GetInstance()
    {
        if (instance == null)
        {
            instance = new GlobalMessageQueue();
        }
        return instance;
    }

    /**
     * @param queID
     */
    public void CreateMessageQueue(int queID)
    {
        LinkedList<PeerNodeMessageType> newMsgQueue;

        newMsgQueue = new LinkedList<PeerNodeMessageType>();

        messageQueues.put(queID, newMsgQueue);
    }

    /**
     * @param queID
     */
    public void DestroyMessageQueue(int queID)
    {
        messageQueues.remove(queID);
    }

    /**
     * Retrieve a message for a specific peer node
     * @param nodeID
     * @return
     */
    public PeerNodeMessageType GetNextMsg(int queID)
    {
        return messageQueues.get(queID).poll();
        // return msgQueue[queID].poll();
    }

    /**
     * Send a message to a specific peer node
     * @param destNodeID
     * @param msg
     */
    public void SendMsg(int destNodeID, PeerNodeMessageType msg)
    {
    	//System.out.println("Global::SendMsg1");
        messageQueues.get(destNodeID).add(msg);
        //System.out.println("Global::SendMsg2");
    }

    /**
     * Sends a message to all peer nodes
     * @param msg
     */
    public void BroadcastMessage(PeerNodeMessageType msg)
    {
        Enumeration<Integer> iterator = messageQueues.keys();

        while (iterator.hasMoreElements())
        {
            int queID = iterator.nextElement();
            messageQueues.get(queID).add(msg);
        }
    }
}
