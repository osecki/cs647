package mapreducer;

public class Master extends Thread
{
    public MRProtocolHandler mrHandler;

    public Master()
    {

    }

    public void SetMRProtocolHandlerRef(MRProtocolHandler reference)
    {
        mrHandler = reference;
    }

    // Method that must be implemented to run worker as a thread
    public void run()
    {
        // Set reference that we're the master
        // mrHandler.SetMasterReference(this);
        mrHandler.UpdateMasterNode(this.hashCode());

        System.out.println("SET MASTER");

        while (true)
        {
            // System.out.println("Master Thread:  " + this.hashCode() +
            // " is running");
        }
    }

    // Method to handle PeerNodeMessageType.SUBMIT_MR_JOB
    public void workerSubmittedJob(String srcFile)
    {
        // Worker has submitted the job, now the master must divide up the work
        // TODO Put algorithm here to decide how to divide up work
    	
    	System.out.println("Master::workerSubmittedJob : " + srcFile);
    	
    	
    }

    /*
     * Method to handle PeerNodeMessageType.MR_JOB_COMPLETE
     * 
     * Each worker sends the MR_JOB_COMPLETE message to the master. When all
     * workers have sent the JOB COMPLETE for a particular job, then notify the
     * Job Client
     */
    public void jobComplete()
    {
        // TODO Tell the job client the work is complete
    }
}
