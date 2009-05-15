package mapreducer;

public class JobClient extends Thread
{
    // Pass down mrHandler
    MRProtocolHandler mrHandler;

    boolean allowJobClientThreadToRun = false;

    public JobClient()
    {

    }

    public void SetMRProtocolHandlerRef(MRProtocolHandler reference)
    {
        mrHandler = reference;
    }

    /**
     * Called from Simulator to suspend the JobClient thread.
     */
    public void sim_HaltJobClientThread()
    {
        allowJobClientThreadToRun = false;
    }

    public void sim_RestartJobClientThread()
    {
        allowJobClientThreadToRun = true;
    }

    public void run()
    {
        String threadName = mrHandler.GetNodeName();
        System.out.println("Starting Thread => " + threadName);
        this.setName(threadName);

        boolean masterExists = false;
        String fileName = ConfigSettings.workTextFile;
        while (true)
        {
            // Need to only submit job if a master exists
            while (!masterExists)
            {
                mrHandler.QueryMasterNode();

                // Sleep 5 seconds to allow for reply
                try
                {
                    Thread.sleep(5000);
                }
                catch (InterruptedException e)
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                if (mrHandler.GetMasterNode() > 0)
                {
                    System.out.println("FOUND MASTER!!! ");
                    masterExists = true;
                    
                    //submit MR job
                    mrHandler.SubmitMRJob(fileName);
                }

            }

            // Using the sim_ methods, the simulator can control the JobClient
            // Thread
            while (allowJobClientThreadToRun)
            {
                // TODO: Do we want to Submit job from here rather than in the
                // above loop

                // System.out.println("Job Client Thread:  " + this.hashCode() +
                // " is running");
            }
        }
    }

    /*
     * Method to handle PeerNodeMessageType.GET_MR_JOB_DATASET
     * 
     * This method retrieves the particular block of data that the requesting
     * Worker node will perform the map/reduce
     */
    public void getDataset()
    {
        // TODO Get the dataset and send back to the requesting worker using
        // the MR_JOB_DATASET_RESULT message
    }

    /*
     * Method to handle PeerNodeMessageType.MR_JOB_REDUCE_RESULT
     * 
     * The results of the various map/reduce results are handled here. This is
     * where the merge will be done.
     */
    public void reduceResult()
    {
        // TODO Method which calculates result of all data
    }
}
