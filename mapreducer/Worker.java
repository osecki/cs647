package mapreducer;

import java.util.ArrayList;
import java.util.LinkedList;

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
    
    public byte[] dataSet;
    public int startIndex;
    public boolean jobDataAvailable;
    
    //Data structure of masters work breakdown
    private ArrayList<JobSubmission> jobAssignments;
    
    //Queue to manage tasks that worker needs to do
    private LinkedList<JobSubmission> myJobList;
    
    public Worker()
    {
    	myJobList = new LinkedList<JobSubmission>();
    }

    public void SetMRProtocolHandlerRef(MRProtocolHandler reference)
    {
        mrHandler = reference;
    }

    // Method that must be implemented to run worker as a thread
    public void run()
    {
        String threadName = mrHandler.GetNodeName();
        EventLogging.info("Starting Worker Thread => " + threadName);
        this.setName(threadName);
        
        this.jobDataAvailable = false;
        
        // Loop on a m/r queue, when stuff is in the queue, do m/r
        while (true)
        {
            // System.out.println("Worker Thread:  " + this.hashCode() +
            // " is running");
        	
        	try 
        	{
        		if(this.jobDataAvailable == true)
        		{
			       processDataset(dataSet);
        		}
			    
        		Thread.sleep(1000);
			} 
        	catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} // Sleep 10 seconds
        }
    }
    
    public void retrieveJobData(PeerNodeMessageType msg)
    {     	
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
    	
    	EventLogging.info("Worker " + this.nodeID + " is beginning processing");
   
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

    	EventLogging.info("Worker " + this.nodeID + " has completed processing: " + count);
    	
    	//done the calculation, send reply
    	this.jobDataAvailable = false;
    	mrHandler.WorkerJobComplete(jobID, count, this.mrHandler.GetMasterNode());
    	
    	
    	//TO DO:
    	//since we are done the job, if we have more jobs on the queue,
    	//get the next one and process
    	
    	if (myJobList.size() > 0)
    	{
    		JobSubmission newJob = myJobList.removeFirst();		//get the new job 
        	this.mrHandler.WorkerGetDataset(newJob.dataSetBlockNumBeginIndex, newJob.dataSetBlockNumEndIndex, newJob.jobClientID);    	
    	}
    }
    
    //This method is used when the master propogates job assignments to all other workers
    //The worker saves this just in case they are promoted to a master
    public void saveJobAssignments(ArrayList<JobSubmission> jobAssign)
    {
    	EventLogging.debug("Worker " + this.nodeID + " has saved master job assignment");
    	jobAssignments = jobAssign;
    }
 
    //This method is used when some other worker fails and the master has detected it
    //When the master assigns the lost chunk to me - an existing worker
    //I will queue it and process
    public void QueueNewJobAssignment(JobSubmission job)
    {
    	myJobList.add(job);
    }
}
