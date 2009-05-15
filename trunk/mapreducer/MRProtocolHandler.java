package mapreducer;

import java.util.ArrayList;

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

    public void ProcessPeerNodeMessage(PeerNodeMessageType msg)
    {
        switch (msg.messageID)
        {
            case PeerNodeMessageType.SUBMIT_MR_JOB:
            {
                master.workerSubmittedJob(msg.srcFileName, msg.wordToSearch, msg.jobClientID);
                break;
            }
            case PeerNodeMessageType.WORKER_START_MR_JOB:
            {
            	//retrieve job data from client
            	worker.retrieveJobData(msg);
            	
                //worker.startMRJob(msg);
                break;
            }
            case PeerNodeMessageType.MR_JOB_COMPLETE:
            {
                master.jobComplete(msg.mrJobID, msg.sourceNode, msg.result);
                break;
            }
            case PeerNodeMessageType.MR_JOB_REDUCE_RESULT:
            {
                jobClient.reduceResult();
                break;
            }
            case PeerNodeMessageType.GET_MR_JOB_DATASET:
            {
                jobClient.getDataset(msg.dataSetBlockNumBeginIndex, msg.dataSetBlockNumEndIndex, msg.sourceNode);
                break;
            }
            case PeerNodeMessageType.MR_JOB_DATASET_REPLY:
            {
                worker.processDataset(msg.dataChunk);
                break;
            }
            case PeerNodeMessageType.HEART_BEAT_PING:
            {
                faultHealth.sendPing();
                break;
            }
            case PeerNodeMessageType.HEART_BEAT_REPLY:
            {
                faultHealth.replyPing();
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
                    System.out.println(nodeName + " saving new master node id:"
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
                    System.out.println("Master received the master_node_query");

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

                // TODO: Need function in MRProtocolHandler class
            	
            	//Save new worker list
            	for (int i = 0; i < msg.workerNodeIDs.length; i++)
            	{
            		//if this node is not already in my list and it's not me, add it
            		if ((!this.workerNodeIDs.contains(msg.workerNodeIDs[i])) && (msg.workerNodeIDs[i] != this.hashCode()))
            			this.workerNodeIDs.add(msg.workerNodeIDs[i]);
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

    public void GetWorkerNodes()
    {
        // TODO
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
        
        // TODO: Notify all peer nodes of new node
        
        //BS TEST CODE
        PeerNodeMessageType msg = new PeerNodeMessageType();
        msg.destNode = PeerNodeMessageType.BROADCAST_DEST_ID;
        msg.messageID = PeerNodeMessageType.UPDATE_WORKER_NODE_LIST;
        msg.workerNodeIDs = workerNodeIDs.toArray(new Integer[workerNodeIDs.size()]);
        commsMgr.SendMsg(msg);        
    }

    /*
     * Used if Master Node. Called by FaultAndHealth if worker node does not
     * respond to Ping
     */
    public void DetectWorkerNodeFailure(int nodeID)
    {
        workerNodeIDs.remove(nodeID);
        // TODO: Call into master
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

    /*
     * Used if Worker node. Worker call this when their piece of the MR job is
     * completed.
     */
    public void WorkerMRJobComplete(int mrJobID, int blockNum)
    {
        PeerNodeMessageType msg;

        msg = new PeerNodeMessageType();

        msg.messageID = PeerNodeMessageType.MR_JOB_COMPLETE;
        msg.destNode = masterNodeID;
        msg.mrJobID = mrJobID;
        //msg.dataSetBlockNum = blockNum;

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
    	  
    	System.out.println("MRProtocolHandler::AssignWorkersJob - Total Words In File: " + totalWords + " To Split Among (" + this.workerNodeIDs.size() + ") Workers = Avg Chunk Size: " + numWordsPerWorker);

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
        	
    		System.out.println("WORKER " + msg.destNode + " (" + i + ") ASSIGNED: " + msg.dataSetBlockNumBeginIndex + " - " + msg.dataSetBlockNumEndIndex + " : " + msg.dataSetSize);
    		
    		//ADD WORKER NODE ID TO MASTER JOB TABLE WITH FALSE
    		//TO INDICATE NO RESPONSE YET
    		
    		
    		this.commsMgr.SendMsg(msg);
    	}
    }
    
    //This method allows the worker to get the dataset from the jobclient
    public void WorkerGetDataset(int begin, int end, int jobClientID)
    {
    	PeerNodeMessageType msg = new PeerNodeMessageType();
    	msg.messageID = PeerNodeMessageType.GET_MR_JOB_DATASET;
    	msg.dataSetBlockNumBeginIndex = begin;
    	msg.dataSetBlockNumEndIndex = end;
    	msg.destNode = jobClientID;
    	this.commsMgr.SendMsg(msg);
    }
    
    //This method allows the job client to send the dataset back to the worker
    public void JobClientSendData(String returnData, int workerDest)
    {   	
    	PeerNodeMessageType msg = new PeerNodeMessageType();
    	msg.messageID = PeerNodeMessageType.MR_JOB_DATASET_REPLY;
    	msg.destNode = workerDest;
    	msg.dataChunk = returnData.getBytes();
    	this.commsMgr.SendMsg(msg);
    }
    
    //This method allows the worker to tell the master he is done
    public void WorkerJobComplete(int jobID, int results, int masterID)
    {
    	PeerNodeMessageType msg = new PeerNodeMessageType();
    	msg.messageID = PeerNodeMessageType.MR_JOB_COMPLETE;
    	msg.destNode = masterID;
    	msg.mrJobID = jobID;
    	msg.result = results;
    	this.commsMgr.SendMsg(msg);
    }
    
    //This method allows the master to tell the job client the work is complete
    public void WorkComplete()
    {
    	
    }
}
