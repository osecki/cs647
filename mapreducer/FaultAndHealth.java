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
	
	private int masterFailureDetectCount;
	
	private int heartbeatIntervalCount;
	
	
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
		masterFailureDetectCount = ConfigSettings.masterNodeFailureTimeout;
		heartbeatIntervalCount = ConfigSettings.heartbeatInterval;
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
			workerNodeTimeOutValues[i] = ConfigSettings.workerNodeFailureTimeout;
		}		
	}
	
	/**
	 * 
	 */
	public void SendPing()
	{
		if(heartbeatIntervalCount == 0)
		{
		   heartbeatIntervalCount = ConfigSettings.heartbeatInterval;	
		try
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
				   
				   mrHandler.DetectWorkerNodeFailure(workerNodeIds[i]);
			   }
		   }
		}
        catch (Exception ex)
        {
            System.out.println(mrHandler.GetNodeName() + " Exception in FaultAndHealth::SendPing " + ex.getMessage());
        }
		}
		else
		{
			heartbeatIntervalCount--;
		}
	}

	
	// IF WORKER node
	public void processHeartBeatPing(int masterNodeId)
	{
		// Each time we are pinged by master, reset the count
		masterFailureDetectCount = ConfigSettings.masterNodeFailureTimeout;		
		
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
			   workerNodeTimeOutValues[i] = ConfigSettings.workerNodeFailureTimeout;
			   break;
			}
		}
	}
	
	// IF WORKER node
    public void checkMasterHeartBeatPing()
    {
    	if(heartbeatIntervalCount == 0)
    	{
    		heartbeatIntervalCount = ConfigSettings.heartbeatInterval;
    		
    	    // On each interval, decrement the masterFailureDetectCount
    	    masterFailureDetectCount--;
    	
            // If we hit a count of zero, then we determine that master has failed
            // since we have not received any pings
           if(masterFailureDetectCount <= 0)
           {
    	       // Send MASTER FAILED message to all other worker nodes
               // System.out.println("Master node has failed.");
        	   for(int i=0; i<workerNodeIds.length; i++)
    		   {    			   
    			  pingMsg = new PeerNodeMessageType();
                  pingMsg.messageID = PeerNodeMessageType.MASTER_NODE_FAILED;
                  pingMsg.sourceNodeType = mrHandler.GetNodeType();
                  pingMsg.destNode = workerNodeIds[i];

                  commsMgr.SendMsg(pingMsg);    			   
    		   }
           }
    	}
    	else
    	{
    		heartbeatIntervalCount--;
    	}
    }
    
 
}
