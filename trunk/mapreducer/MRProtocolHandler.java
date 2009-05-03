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

    public void ProcessPeerNodeMessage(PeerNodeMessageType msg)
    {
        switch (msg.messageID)
        {
            case PeerNodeMessageType.SUBMIT_MR_JOB:
            {
                // TODO: Need function in Master class
                break;
            }
            case PeerNodeMessageType.WORKER_START_MR_JOB:
            {
                // TODO: Need function in Worker class
                break;
            }
            case PeerNodeMessageType.MR_JOB_COMPLETE:
            {
                // TODO: Need function in Master class
                break;
            }
            case PeerNodeMessageType.MR_JOB_REDUCE_RESULT:
            {
                // TODO: Need function in JobClient class
                break;
            }
            case PeerNodeMessageType.GET_MR_JOB_DATASET:
            {
                // TODO: Need function in JobClient class
                break;
            }
            case PeerNodeMessageType.MR_JOB_DATASET_REPLY:
            {
                // TODO: Need function in Worker class
                break;
            }
            case PeerNodeMessageType.HEART_BEAT_PING:
            {
                // TODO: Need function in FaultAndHealth class
                break;
            }
            case PeerNodeMessageType.HEART_BEAT_REPLY:
            {
                // TODO: Need function in FaultAndHealth class
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
                // TODO: Need function in MRProtocolHandler class
                UpdateMasterNode(msg.masterNodeID);
                break;
            }
            case PeerNodeMessageType.MASTER_NODE_QUERY:
            {
                if (nodeType == PeerNodeRoleType.MASTER)
                {
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
                UpdateMasterNode(msg.masterNodeID);
                break;
            }
            case PeerNodeMessageType.UPDATE_WORKER_NODE_LIST:
            {
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

    /*
     * Used if JobClient node.
     */
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

}
