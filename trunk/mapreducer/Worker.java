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
    
    public int startIndex;
    
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
        
        // Loop on a m/r queue, when stuff is in the queue, do m/r
        while (true)
        {
            // System.out.println("Worker Thread:  " + this.hashCode() +
            // " is running");
        	
        	try 
        	{
        		if (readyToWork())
        		{
			       processDataset(myJobList.removeFirst());
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
    	//save an entry in myJobList
    	JobSubmission jobSub = new JobSubmission();
    	jobSub.dataChunkID = msg.dataChunkID;
    	jobSub.dataSetBlockNumBeginIndex = msg.dataSetBlockNumBeginIndex;
    	jobSub.dataSetBlockNumEndIndex = msg.dataSetBlockNumEndIndex;
    	jobSub.jobClientID = msg.jobClientID;
    	jobSub.jobID = msg.mrJobID;
    	myJobList.add(jobSub);
    	
    	//save the word to search
    	wordToSearch = msg.wordToSearch;
    	
    	// Sends the GET_MR_JOB_DATASET message to the job client
    	this.mrHandler.WorkerGetDataset(msg.dataSetBlockNumBeginIndex, msg.dataSetBlockNumEndIndex, msg.jobClientID, msg.dataChunkID);
    }

    // Method to handle PeerNodeMessageType.MR_JOB_DATASET_REPLY
    public void processDataset(JobSubmission job) throws InterruptedException
    { 	
    	EventLogging.info("Worker " + this.nodeID + " is beginning processing chunk " + job.dataChunkID);
   
    	int count = 0;
    	String data = new String(job.dataset);
    	String words[] = data.split(" ");
    	
    	for (int i = 0; i < words.length; i++)
    		if (words[i].equals(wordToSearch))
    			count = count + 1;    	
		
		//introduce some delay in the processing
		Thread.sleep(10000);

    	EventLogging.info("Worker " + this.nodeID + " has completed processing: " + count);
    	
    	//done the calculation, send reply
    	mrHandler.WorkerJobComplete(job.jobID, count, this.mrHandler.GetMasterNode(), job.dataChunkID);
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
    
    public void SetChunkDataset(byte[] dataSet, int dataChunkID)
    {
    	for (int i = 0; i < myJobList.size(); i++)
    	{
    		if (myJobList.get(i).dataChunkID == dataChunkID)
    		{
    			myJobList.get(i).dataset = dataSet;
    			break;
    		}
    	}
    }
    
    private boolean readyToWork()
    {
    	boolean ret = false;

    	for (int i = 0; i < myJobList.size(); i++)
    	{
    		if (myJobList.get(i).dataset != null)
    		{
    			ret = true;
    			break;
    		}
    	}
    	
    	return ret;
    }
}
