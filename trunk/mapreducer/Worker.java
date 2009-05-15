package mapreducer;

public class Worker extends Thread
{
    /*
     * TODO THE BIG QUESTION...Do we want to run the worker as another thread so
     * that the message queue could still be read and more jobs be scheduled for
     * the worker while it is working on a job
     */

    public MRProtocolHandler mrHandler;

    public Worker()
    {

    }

    public void SetMRProtocolHandlerRef(MRProtocolHandler reference)
    {
        mrHandler = reference;
    }

    // Method that must be implemented to run worker as a thread
    public void run()
    {
        String threadName = mrHandler.GetNodeName();
        System.out.println("Starting Worker Thread => " + threadName);
        this.setName(threadName);

        //BS TEST
        this.mrHandler.sim_NewWorkerNodeConnected(this.hashCode());
        
        // Loop on a m/r queue, when stuff is in the queue, do m/r
        while (true)
        {
            // System.out.println("Worker Thread:  " + this.hashCode() +
            // " is running");
        }
    }

    // Method to handle PeerNodeMessageType.WORKER_START_MR_JOB
    public void startMRJob()
    {
        // TODO Have worker begin to do the MR job.
        // Sends the GET_MR_JOB_DATASET message to the job client
    }

    // Method to handle PeerNodeMessageType.MR_JOB_DATASET_REPLY
    public void processDataset()
    {
        // TODO The dataset for this worker has arrived, start the m/r job. Put
        // on queue for thread to process????
    }
}
