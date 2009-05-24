package mapreducer;

import java.util.ArrayList;
import java.util.Hashtable;

public class MRProtocolHandler
{
    public P2PCommsManager commsMgr;
    public Master master;
    public Worker worker;
    public JobClient jobClient;
    public FaultAndHealth faultHealth;
    public Simulator sim;

    private PeerNodeRoleType nodeType;
    private String nodeName;

    private int masterNodeID;
    private ArrayList<Integer> workerNodeIDs;

    private PeerNodeMessageType replyMsg;

    public MRProtocolHandler()
    {
        workerNodeIDs = new ArrayList<Integer>();
    }

    public void SetSimulatorReference(Simulator reference)
    {
        sim = reference;
    }

    public void SetP2PCommsManagerReference(P2PCommsManager reference)
    {
        commsMgr = reference;
    }

    public void SetMasterReference(Master reference)
    {
        master = reference;
    }

    public void SetWorkerReference(Worker reference)
    {
        worker = reference;
    }

    public void SetJobClientReference(JobClient reference)
    {
        jobClient = reference;
    }

    public void SetFaultAndHealthReference(FaultAndHealth reference)
    {
        faultHealth = reference;
    }

    public void SetNodeType(PeerNodeRoleType type)
    {
        nodeType = type;
    }

    public void SetNodeName()
    {
        nodeName = nodeType.toString() + ":" + String.valueOf(commsMgr.GetNodeID());
    }

    public String GetNodeName()
    {
        return nodeName;
    }
    
    public void PerformHouseKeeping()
    {
    	if(this.nodeType == PeerNodeRoleType.WORKER)
    	{
    	   // Run routine to check that master pings are being recieved
    		faultHealth.checkMasterHeartBeatPing();
    	}
    	if(this.nodeType == PeerNodeRoleType.MASTER)
    	{
    	   // Run routine to send ping to workers
    		faultHealth.SendPing();
    	}
    }

    public void ProcessPeerNodeMessage(PeerNodeMessageType msg)
    {
        switch (msg.messageID)
        {
        	case PeerNodeMessageType.PROPOGATE_JOB_ASSIGNMENTS:
        	{
        		if (nodeType == PeerNodeRoleType.WORKER)
        		{
        			worker.saveJobAssignments(msg.jobAssignment);
        		}
        		break;
        	}
            case PeerNodeMessageType.SUBMIT_MR_JOB:
            {
                master.workerSubmittedJob(msg.srcFileName, msg.wordToSearch, msg.jobClientID);
                break;
            }
            case PeerNodeMessageType.WORKER_START_MR_JOB:
            {
            	//retrieve job data from client
            	worker.retrieveJobData(msg);
                break;
            }
            case PeerNodeMessageType.MR_JOB_COMPLETE:
            {
                master.jobComplete(msg.mrJobID, msg.sourceNode, msg.result, msg.dataChunkID);
                break;
            }
            case PeerNodeMessageType.MR_JOB_REDUCE_RESULT:
            {
                jobClient.reduceResult(msg.workerResults);
                break;
            }
            case PeerNodeMessageType.GET_MR_JOB_DATASET:
            {
                jobClient.getDataset(msg.dataSetBlockNumBeginIndex, msg.dataSetBlockNumEndIndex, msg.sourceNode, msg.dataChunkID);
                break;
            }
            case PeerNodeMessageType.MR_JOB_DATASET_REPLY:
            {            	
            	worker.SetChunkDataset(msg.dataChunk, msg.dataChunkID);
                break;
            }
            case PeerNodeMessageType.HEART_BEAT_PING:
            {
            	if(nodeType == PeerNodeRoleType.WORKER)
            	{                   
                   faultHealth.processHeartBeatPing(this.masterNodeID);
            	}
                
                break;
            }
            case PeerNodeMessageType.HEART_BEAT_REPLY:
            {            	
                faultHealth.processHeartBeatPingReply(msg.sourceNode);
                break;
            }
            case PeerNodeMessageType.MASTER_NODE_FAILED:
            {
                // TODO: Need function in MRProtocolHandler class
                if (nodeType == PeerNodeRoleType.WORKER)
                {
                    // Worker nodes are the only nodes that care about master
                    // and will determine new master node.
                }
                break;
            }
            case PeerNodeMessageType.INITIATE_NEW_MASTER_PROTOCOL:
            {
                // TODO: Need function in MRProtocolHandler class
                if (nodeType == PeerNodeRoleType.WORKER)
                {
                    // Worker nodes are the only nodes that care about master
                    // and will determine new master node.
                }
                break;
            }
            case PeerNodeMessageType.NEW_MASTER_NODE:
            {
                // Only Worker and JobClient nodes need to be notified of a new
                // master node
                if (nodeType != PeerNodeRoleType.MASTER)
                {
                    UpdateMasterNode(msg.masterNodeID);
                   EventLogging.debug(nodeName + " saving new master node id:"
                            + String.valueOf(masterNodeID));
                }
                break;
            }
            case PeerNodeMessageType.MASTER_NODE_QUERY:
            {
                // System.out.println(nodeType +
                // String.valueOf(commsMgr.GetNodeID()));

                if (nodeType == PeerNodeRoleType.MASTER)
                {
                    EventLogging.debug("Master received the master_node_query");

                    replyMsg = new PeerNodeMessageType();

                    replyMsg.messageID = PeerNodeMessageType.MASTER_NODE_QUERY_REPLY;
                    replyMsg.sourceNodeType = nodeType;
                    replyMsg.destNode = msg.sourceNode;

                    commsMgr.SendMsg(replyMsg);
                }

                break;
            }
            case PeerNodeMessageType.MASTER_NODE_QUERY_REPLY:
            {
                // On the reply, set the master node
                UpdateMasterNode(msg.sourceNode);
                break;
            }
            case PeerNodeMessageType.UPDATE_WORKER_NODE_LIST:
            {
                // The Worker node and Master node need to maintain this list
            	if((nodeType == PeerNodeRoleType.MASTER) || 
            	  (nodeType == PeerNodeRoleType.WORKER))
            	{
            		this.workerNodeIDs.clear();
            		
            		
            	   //Save new worker list
            	   for (int i = 0; i < msg.workerNodeIDs.length; i++)
            	   {
            		   //if this node is not already in my list and it's not me, add it
            	//	   if ((!this.workerNodeIDs.contains(msg.workerNodeIDs[i])) && (msg.workerNodeIDs[i] != this.hashCode()))
            			   this.workerNodeIDs.add(msg.workerNodeIDs[i]);
               	   }
            	
            	   // Need to let the FaultHealt object know who the worker nodes are
            	   faultHealth.UpdateWorkerNodeList(workerNodeIDs);
            	}
                break;
            }
        }
    }

    public void UpdateMasterNode(int newMaster)
    {
        masterNodeID = newMaster;
    }

    public void UpdateWorkerNodes()
    {
        // TODO
    }

    public int GetMasterNode()
    {
        return masterNodeID;
    }

    public ArrayList<Integer> GetWorkerNodes()
    {
        // TODO
    	return workerNodeIDs;
    }
    
    public int getWorkerCount()
    {
    	return workerNodeIDs.size(); 
    }

    public PeerNodeRoleType GetNodeType()
    {
        return nodeType;
    }

    /*
     * Used if Master Node. Simulator calls this when it creates a new worker
     * node
     */
    public void sim_NewWorkerNodeConnected(int nodeID)
    {
        workerNodeIDs.add(nodeID);        
    }
    


    /*
     * Used if Master Node. Called by FaultAndHealth if worker node does not
     * respond to Ping
     */
    public void DetectWorkerNodeFailure(int nodeID)
    {    	 
    	boolean removedNode = false;
    	
       	//find the index in the list for this nodeID so we can remove it
       	for (int i = 0 ; i < workerNodeIDs.size(); i++)
       	{
       		if (workerNodeIDs.get(i) == nodeID)
       		{
       			workerNodeIDs.remove(i);
       			removedNode = true;
       			break;
       		}
       	}
        
       	if (removedNode)
   	        master.WorkerFailureDetected(nodeID);
       	
    }

    /*
     * Used if Worker node. Called by FaultAndHealth if master pings no longer
     * received
     */
    public void DetectMasterNodeFailure()
    {
        replyMsg = new PeerNodeMessageType();

        replyMsg.destNode = PeerNodeMessageType.BROADCAST_DEST_ID;
        replyMsg.messageID = PeerNodeMessageType.MASTER_NODE_FAILED;

        commsMgr.SendMsg(replyMsg);
    }

    /* Used if JobClient node. */
    public void SubmitMRJob(String filename, String wordToSearch)
    {
        PeerNodeMessageType msg;

        msg = new PeerNodeMessageType();

        msg.messageID = PeerNodeMessageType.SUBMIT_MR_JOB;
        msg.destNode = masterNodeID;
        msg.jobClientID = commsMgr.GetNodeID();
        msg.srcFileName = filename;
        msg.wordToSearch = wordToSearch;
        msg.sourceNodeType = nodeType;

        commsMgr.SendMsg(msg);
    }

    // Interface to let a node send a message to query for the master node
    public void QueryMasterNode()
    {
        PeerNodeMessageType msg = new PeerNodeMessageType();
        msg.messageID = PeerNodeMessageType.MASTER_NODE_QUERY;
        msg.destNode = PeerNodeMessageType.BROADCAST_DEST_ID;
        commsMgr.SendMsg(msg);
    }

    /*
     * The newly elected master node calls this to notify other nodes who the
     * new master is
     */
    public void BroadcastNewMasterNode()
    {
        PeerNodeMessageType msg = new PeerNodeMessageType();
        msg.messageID = PeerNodeMessageType.NEW_MASTER_NODE;
        msg.destNode = PeerNodeMessageType.BROADCAST_DEST_ID;
        msg.masterNodeID = commsMgr.GetNodeID();
        commsMgr.SendMsg(msg);
    }

    public void BroadcastWorkerNodeList()
    {
        PeerNodeMessageType msg = new PeerNodeMessageType();
        msg.destNode = PeerNodeMessageType.BROADCAST_DEST_ID;
        msg.messageID = PeerNodeMessageType.UPDATE_WORKER_NODE_LIST;
        msg.workerNodeIDs = workerNodeIDs.toArray(new Integer[workerNodeIDs.size()]);
        commsMgr.SendMsg(msg);    	
    }
    
    //This method is called by the master to tell assign the workers
    //chunks of work and instruct them to begin
    //Input : words - total words in file
    public void AssignWorkersJob(int jobID, ArrayList<String> words, String wordToSearch, int jobClientID)
    {
    	
    	int totalWords = words.size();
    	int currentBlockBegin = 1;
    	int currentBlockEnd = 0;
    	
    	//for now, split as evenly across all workers - last worker may get more due to division
    	//will need to calculate
    	int numWordsPerWorker = words.size() / this.workerNodeIDs.size();
    	  
    	EventLogging.debug("MRProtocolHandler::AssignWorkersJob - Total Words In File: " + totalWords + " To Split Among (" + this.workerNodeIDs.size() + ") Workers = Avg Chunk Size: " + numWordsPerWorker);

    	//loop through each worker and send message w/ assigned chunk	
    	for (int i = 0; i < this.workerNodeIDs.size(); i++)
    	{
    	   	PeerNodeMessageType msg = new PeerNodeMessageType();
    	   	msg.mrJobID = jobID;
        	msg.destNode = this.workerNodeIDs.get(i);
        	msg.messageID = PeerNodeMessageType.WORKER_START_MR_JOB;

        	//set job client ID
        	msg.jobClientID = jobClientID;
        	
        	//save job ID
        	msg.mrJobID = jobID;
        	
        	//save work chunk ID
        	msg.dataChunkID = (i + 1);
        	
        	//Inform them of what to search for
        	msg.wordToSearch = wordToSearch;
        	
        	//set block bounds
        	msg.dataSetBlockNumBeginIndex = currentBlockBegin;	
        	msg.dataSetBlockNumEndIndex = currentBlockBegin + numWordsPerWorker;
        	msg.dataSetSize = numWordsPerWorker;
        		
        	//increment / decrement counts
        	currentBlockBegin = currentBlockBegin + numWordsPerWorker + 1;
        	
        	//Account for last worker which may get assigned less
        	if (msg.dataSetBlockNumEndIndex > totalWords)
        	{
        		msg.dataSetBlockNumEndIndex = totalWords;
        		msg.dataSetSize = (msg.dataSetBlockNumEndIndex - msg.dataSetBlockNumBeginIndex);
        	}
        	
    		EventLogging.info("WORKER " + msg.destNode + " Assigned Chunk (" + msg.dataChunkID + "): " + msg.dataSetBlockNumBeginIndex + " - " + msg.dataSetBlockNumEndIndex);
 		
    		//Save job information in a structure such that we can propogate to all 
    		//workers
    		JobSubmission jobSub = new JobSubmission();
    		jobSub.jobID = jobID;
    		jobSub.jobClientID = jobClientID;
    		jobSub.dataChunkID = msg.dataChunkID;
    		jobSub.dataSetBlockNumBeginIndex = msg.dataSetBlockNumBeginIndex;
    		jobSub.dataSetBlockNumEndIndex = msg.dataSetBlockNumEndIndex;
    		jobSub.workerNodeID = msg.destNode;
    		jobSub.result = -1;
    		
    		//Add assignment to master nodes list
    		this.master.jobAssignments.add(jobSub);    		
    		
    		//Send the message
    		this.commsMgr.SendMsg(msg);
    	}
    }
    
    //This method allows the worker to get the dataset from the jobclient
    public void WorkerGetDataset(int begin, int end, int jobClientID, int chunkID)
    {
    	PeerNodeMessageType msg = new PeerNodeMessageType();
    	msg.messageID = PeerNodeMessageType.GET_MR_JOB_DATASET;
    	msg.dataSetBlockNumBeginIndex = begin;
    	msg.dataSetBlockNumEndIndex = end;
    	msg.destNode = jobClientID;
    	msg.dataChunkID = chunkID;
    	this.commsMgr.SendMsg(msg);
    }
    
    //This method allows the job client to send the dataset back to the worker
    public void JobClientSendData(String returnData, int workerDest, int dataChunkID)
    {	
    	PeerNodeMessageType msg = new PeerNodeMessageType();
    	msg.messageID = PeerNodeMessageType.MR_JOB_DATASET_REPLY;
    	msg.destNode = workerDest;
    	msg.dataChunk = returnData.getBytes();
    	msg.dataChunkID = dataChunkID;
    	this.commsMgr.SendMsg(msg);
    }
    
    //This method allows the worker to tell the master he is done
    public void WorkerJobComplete(int jobID, int results, int masterID, int dataChunkID)
    {
    	PeerNodeMessageType msg = new PeerNodeMessageType();
    	msg.messageID = PeerNodeMessageType.MR_JOB_COMPLETE;
    	msg.destNode = masterID;
    	msg.mrJobID = jobID;
    	msg.result = results;
    	msg.dataChunkID = dataChunkID;
    	this.commsMgr.SendMsg(msg);
    }
    
    //This method allows the master to tell the job client the work is complete
    public void WorkComplete(int jobClientID, int jobID, ArrayList<Integer> totalResults)
    {
    	PeerNodeMessageType msg = new PeerNodeMessageType();
    	msg.messageID = PeerNodeMessageType.MR_JOB_REDUCE_RESULT;
    	msg.destNode = jobClientID;
    	msg.mrJobID = jobID;
    	msg.workerResults = totalResults;
    	this.commsMgr.SendMsg(msg);
    }
    
    //This method allows the master to send out the job assignment list to all nodes
    //such that if master dies, worker can take over
    public void SendOutJobAssignmentList(ArrayList<JobSubmission> jobAssignments)
    {
    	PeerNodeMessageType msg = new PeerNodeMessageType();
    	msg.messageID = PeerNodeMessageType.PROPOGATE_JOB_ASSIGNMENTS;
    	msg.destNode = PeerNodeMessageType.BROADCAST_DEST_ID;
    	msg.jobAssignment = jobAssignments;
    	this.commsMgr.SendMsg(msg);
    }
    
    public void AssignNewWorkerJob(int jobID, int newWorkerNodeID, int jobClientID, int dataChunkID, int dataBlockBegin, int dataBlockEnd)
    {
	   	PeerNodeMessageType msg = new PeerNodeMessageType();
	   	msg.mrJobID = jobID;
    	msg.destNode = newWorkerNodeID;
    	msg.messageID = PeerNodeMessageType.WORKER_START_MR_JOB;
    	msg.jobClientID = jobClientID;
    	msg.mrJobID = jobID;
    	msg.dataChunkID = dataChunkID;
    	msg.wordToSearch = ConfigSettings.wordToSearch;
    	msg.dataSetBlockNumBeginIndex = dataBlockBegin;	
    	msg.dataSetBlockNumEndIndex = dataBlockEnd;
    	msg.dataSetSize = (dataBlockEnd - dataBlockBegin);
    	
		EventLogging.info("Worker " + msg.destNode + " Assigned Chunk (" + msg.dataChunkID + "): " + msg.dataSetBlockNumBeginIndex + " - " + msg.dataSetBlockNumEndIndex);
    	
		this.commsMgr.SendMsg(msg);
    }
}
