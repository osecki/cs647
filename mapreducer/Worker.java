package mapreducer;

public class Worker extends Thread
{
    /*
     * TODO THE BIG QUESTION...Do we want to run the worker as another thread so
     * that the message queue could still be read and more jobs be scheduled for
     * the worker while it is working on a job
     */

    public MRProtocolHandler mrHandler;
    public int nodeID;
    public String wordToSearch;
    public int jobID;
    
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
        this.mrHandler.sim_NewWorkerNodeConnected(this.nodeID);
        
        // Loop on a m/r queue, when stuff is in the queue, do m/r
        //while (true)
        //{
            // System.out.println("Worker Thread:  " + this.hashCode() +
            // " is running");
        //}
    }
    
    public void retrieveJobData(PeerNodeMessageType msg)
    {
    	System.out.println("Retrieve From Job Client: " + msg.jobClientID);
     	
    	//save jobID for now
    	this.jobID = msg.mrJobID;
    	
    	//save the word to search
    	wordToSearch = msg.wordToSearch;
    	
    	// Sends the GET_MR_JOB_DATASET message to the job client
    	this.mrHandler.WorkerGetDataset(msg.dataSetBlockNumBeginIndex, msg.dataSetBlockNumEndIndex, msg.jobClientID);
    }

    // Method to handle PeerNodeMessageType.MR_JOB_DATASET_REPLY
    public void processDataset(byte[] dataSet)
    {
        // TODO The dataset for this worker has arrived, start the m/r job. Put
        // on queue for thread to process????
    	
    	System.out.println("Worker " + this.nodeID + " has received dataset and needs to search: " + wordToSearch);
   
    	//for now just process and get a result
    	
    	int count = 0;
    	String data = new String(dataSet);
    	String words[] = data.split(" ");
    	
    	for (int i = 0; i < words.length; i++)
    	{
    		if (words[i].equals(wordToSearch))
    		{
    			count = count + 1;
    		}
    	}
    	
    	//done the calculation, send reply
    	mrHandler.WorkerJobComplete(jobID, count, this.mrHandler.GetMasterNode());
    }
}
