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
                master.workerSubmittedJob(msg.srcFileName);
                break;
            }
            case PeerNodeMessageType.WORKER_START_MR_JOB:
            {
                worker.startMRJob();
                break;
            }
            case PeerNodeMessageType.MR_JOB_COMPLETE:
            {
                master.jobComplete();
                break;
            }
            case PeerNodeMessageType.MR_JOB_REDUCE_RESULT:
            {
                jobClient.reduceResult();
                break;
            }
            case PeerNodeMessageType.GET_MR_JOB_DATASET:
            {
                jobClient.getDataset();
                break;
            }
            case PeerNodeMessageType.MR_JOB_DATASET_REPLY:
            {
                worker.processDataset();
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
    public void SubmitMRJob(String filename)
    {
        PeerNodeMessageType msg;

        msg = new PeerNodeMessageType();

        msg.messageID = PeerNodeMessageType.SUBMIT_MR_JOB;
        msg.destNode = masterNodeID;
        msg.jobClientID = commsMgr.GetNodeID();
        msg.srcFileName = filename;
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
        msg.dataSetBlockNum = blockNum;

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
}
