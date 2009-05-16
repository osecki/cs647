package mapreducer;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

public class FaultAndHealth 
{
	private MRProtocolHandler mrHandler;
	private P2PCommsManager commsMgr;
	
	private PeerNodeMessageType pingMsg;
	
	private int[] workerNodeIds;
	private int[] workerNodeTimeOutValues;
	
	private static final int MASTER_FAILURE_DETECT_TIMEOUT = 10;  // 100's millisec
	private static final int WORKER_FAILURE_DETECT_TIMEOUT = 10;  // 100's millisec
	
	private int masterFailureDetectCount;
	
	
    /**
     * @param reference
     */
    public void SetMRProtocolHandlerRef(MRProtocolHandler reference)
    {
        mrHandler = reference;
    }
    
    public void SetP2PCommsManagerReference(P2PCommsManager reference)
    {
        commsMgr = reference;
    }
	
	public FaultAndHealth ()
	{
		masterFailureDetectCount = MASTER_FAILURE_DETECT_TIMEOUT;
	}
	
	/**
	 * 
	 * @param workerNodes
	 */
	public void UpdateWorkerNodeList(ArrayList<Integer> workerNodes)
	{
		workerNodeIds = new int[workerNodes.size()];
		workerNodeTimeOutValues = new int[workerNodes.size()];
		
		for(int i=0; i<workerNodes.size(); i++)
		{ 
			workerNodeIds[i] = workerNodes.get(i);
			workerNodeTimeOutValues[i] = WORKER_FAILURE_DETECT_TIMEOUT;
		}		
	}
	
	/**
	 * 
	 */
	public void SendPing()
	{
		for(int i=0; i<workerNodeIds.length; i++)
		{
			workerNodeTimeOutValues[i]--;
			
			if(workerNodeTimeOutValues[i] > 0)
			{
				// Send ping
				pingMsg = new PeerNodeMessageType();
                pingMsg.messageID = PeerNodeMessageType.HEART_BEAT_PING;
                pingMsg.sourceNodeType = mrHandler.GetNodeType();
                pingMsg.destNode = workerNodeIds[i];

                commsMgr.SendMsg(pingMsg);
			}
			else
			{
				// TODO: Worker node failed, notify master 
				System.out.println("Worker node has failed. ID="+workerNodeIds[i]);
			}
		}
	}

	
	// IF WORKER node
	public void processHeartBeatPing(int masterNodeId)
	{
		// Each time we are pinged by master, reset the count
		masterFailureDetectCount = MASTER_FAILURE_DETECT_TIMEOUT;		
		
		pingMsg = new PeerNodeMessageType();
		pingMsg.messageID = PeerNodeMessageType.HEART_BEAT_REPLY;
		pingMsg.sourceNodeType = mrHandler.GetNodeType();
		pingMsg.destNode = masterNodeId;

        commsMgr.SendMsg(pingMsg);
	}
	
	
	/**
	 *  Used by MASTER node when ping reply received from worker 
	 */
	public void processHeartBeatPingReply(int workerID)
	{
		// Search for matching id in list, once found, reset the timeout value
		for(int i=0; i<workerNodeIds.length; i++)
		{
			if(workerNodeIds[i] == workerID)
			{
			   workerNodeTimeOutValues[i] = WORKER_FAILURE_DETECT_TIMEOUT;
			   break;
			}
		}
	}
	
	// IF WORKER node
    public void checkMasterHeartBeatPing()
    {
    	// On each interval, decrement the masterFailureDetectCount
    	masterFailureDetectCount--;
    	
       // If we hit a count of zero, then we determine that master has failed
       // since we have not received any pings
       if(masterFailureDetectCount <= 0)
       {
    	   // TODO: Call MRHandler to initiate Master Election logic
    	   //System.out.println("Master node has failed.");
       }
    }
    
 
}
