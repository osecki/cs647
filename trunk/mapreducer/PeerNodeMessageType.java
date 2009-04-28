package mapreducer;

public class PeerNodeMessageType
{
    // Message IDs
    public static final int SUBMIT_MR_JOB = 100;
    public static final int WORKER_START_MR_JOB = 101;
    public static final int MR_JOB_COMPLETE = 102;
    public static final int MASTER_NODE_FAILED = 103;

    // Dest Node IDs for special messages
    public static final int BROADCAST_DEST_ID = 255;

    public int messageID;
    public int sourceNode;
    public int destNode;

    public PeerNodeMessageType()
    {

    }
}
