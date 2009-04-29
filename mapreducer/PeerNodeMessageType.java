package mapreducer;

public class PeerNodeMessageType
{
    // Message IDs
    public static final int SUBMIT_MR_JOB = 100;
    public static final int WORKER_START_MR_JOB = 101;
    public static final int MR_JOB_COMPLETE = 102;
    public static final int GET_MR_JOB_DATASET = 103;
    public static final int MR_JOB_REDUCE_RESULT = 104;
    public static final int MR_JOB_DATASET_REPLY = 105;

    public static final int MASTER_NODE_FAILED = 200;
    public static final int NEW_MASTER_NODE = 201;
    public static final int INITIATE_NEW_MASTER_PROTOCOL = 202;
    public static final int MASTER_NODE_QUERY = 203; // Used by job client to
    // find master node
    public static final int MASTER_NODE_QUERY_REPLY = 204; // Reply back to job
    // client

    public static final int HEART_BEAT_PING = 300;
    public static final int HEART_BEAT_REPLY = 301;

    public static final int UPDATE_WORKER_NODE_LIST = 400;

    // Dest Node IDs for special messages
    public static final int BROADCAST_DEST_ID = 255;

    public int messageID;
    public int sourceNode;
    public PeerNodeRoleType sourceNodeType;
    public int destNode;

    // MR job related fields
    public int jobClientID; // know where to get data and put results
    public String srcFileName;
    public int dataSetSize; // size of the chunk of data to map/reduce
    public int dataSetBlockNum; // Id of the particular chunk?

    // Master Node Election Logic related fields

    // General P2P Network information related fields
    public int newMasterNodeID;
    public int[] workerNodeIDs;

    public PeerNodeMessageType()
    {

    }
}
