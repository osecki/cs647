package mapreducer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

public class Master extends Thread
{
    public MRProtocolHandler mrHandler;
    public int nodeID;
    public static int jobID = 0;

	//List of all workers assignments for this job submission
	public ArrayList<JobSubmission> jobAssignments;
    
    //Table to keep key=jobID value=jobClientID
    public Hashtable<Integer, Integer> jobClientMap = new Hashtable<Integer,Integer>();
    
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
        String threadName = mrHandler.GetNodeName();
        EventLogging.info("Starting Master Thread => " + threadName);
        mrHandler.UpdateMasterNode(this.nodeID);
    }

    // Method to handle PeerNodeMessageType.SUBMIT_MR_JOB
    public void workerSubmittedJob(String srcFile, String wordToSearch, int jobClientID)
    {
    	 jobAssignments = new ArrayList<JobSubmission>();    	
    	
        // Worker has submitted the job, now the master must divide up the work
        // TODO Put algorithm here to decide how to divide up work
    	
    	ArrayList<String> words = new ArrayList<String>();
    	String text;
    	
    	EventLogging.debug("Master received job request - splitting job among workers");
    	
    	//Maybe read the number of words in the file and split among workers for now
    	
		try 
		{
			FileReader fileReader = new FileReader(srcFile);
			BufferedReader reader = new BufferedReader(fileReader);

	    	while ((text = reader.readLine()) != null)
	    	{
	    		String[] temp = text.split(" ");
	    
	    		//Add each word to a list for splitting up into chunks later
	    		for (int j = 0; j < temp.length; j++)
	    			words.add(temp[j]);
	    	}
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}

		//Get a new JobID
		jobID = jobID + 1;

		//create new entry in job client map
		this.jobClientMap.put(jobID, jobClientID);
		
    	//Tell workers to get the data sets
    	this.mrHandler.AssignWorkersJob(jobID, words, wordToSearch, jobClientID);
    	
    	//Send out assignment list to all nodes 
    	this.mrHandler.SendOutJobAssignmentList(this.jobAssignments,jobID, jobClientMap);
    }
    
    //This method is used when the master propogates job assignments to all other workers
    //The worker saves this just in case they are promoted to a master
    public void saveJobAssignments(PeerNodeMessageType msg)
    {
    	EventLogging.debug("Worker " + this.nodeID + " has saved master job assignment");
    	jobAssignments = msg.jobAssignment;
    	
    	jobID = msg.latestJobID;
    	
    	jobClientMap = msg.jobClientMap;
    }

    /*
     * Method to handle PeerNodeMessageType.MR_JOB_COMPLETE
     * 
     * Each worker sends the MR_JOB_COMPLETE message to the master. When all
     * workers have sent the JOB COMPLETE for a particular job, then notify the
     * Job Client
     */
    public void jobComplete(int jobID, int workerID, int result, int dataChunkID)
    {
    	boolean done = true;
    	ArrayList<Integer> totalResults = new ArrayList<Integer>();
    	
    	EventLogging.info("Master has received worker " + workerID + " results for chunk " + dataChunkID);

    	for (int i = 0; i < jobAssignments.size(); i++)
    	{
    		JobSubmission job = jobAssignments.get(i);
    		    		
    		//update our result
    		if ((job.workerNodeID == workerID) && (job.jobID == jobID) && (job.dataChunkID == dataChunkID))
    			job.result = result;
		
    		//check to see if all workers are done
    		if (job.jobID == jobID)
    			if (job.result == -1)
    				done = false;
    		
    		totalResults.add(job.result);
    	}
    	
        // Tell the job client the work is complete
    	if (done)
    	{
        	EventLogging.info("Master notified that all workers are finished");
    			
    		int jobClientID = this.jobClientMap.get(jobID);
    		mrHandler.WorkComplete(jobClientID, jobID, totalResults);
    	}
    	else
    	{		
			//propogate this result to all other workers so they can update their
			//job assignment table
    		
    		this.mrHandler.SendOutJobAssignmentList(this.jobAssignments, jobID, jobClientMap);    		
    	}

    }
    
    public void WorkerFailureDetected(int workerNodeID)
    {
    	EventLogging.info("Master node has been notified that worker " + workerNodeID + " has failed");
    	
    	//find the job assignment for this worker
    	for (int i = 0; i < jobAssignments.size(); i++)
    	{
    		JobSubmission job = jobAssignments.get(i);
    		
    		if (job.workerNodeID == workerNodeID)
    		{
    			//Grab a random worker for now
    			//Probably will need to in the future distinguish between
    			//free workers and working workers
    			
    			ArrayList<Integer> workerNodes = mrHandler.GetWorkerNodes();
    			
    			//grab a workerNode and correct the jobAssignments data structure
    			int newWorkerNodeID = workerNodes.get(0);		//FIX ME
    			
    			//send out the new jobAssignments data structure to all nodes
    			jobAssignments.get(i).workerNodeID = newWorkerNodeID;
    			mrHandler.SendOutJobAssignmentList(jobAssignments, jobID, jobClientMap);
    			
    			//send out a message to the new worker to tell him to grab the dataset
    			mrHandler.AssignNewWorkerJob(job.jobID, newWorkerNodeID, job.jobClientID, job.dataChunkID, job.dataSetBlockNumBeginIndex, job.dataSetBlockNumEndIndex);
    			
    			break;
    		}
    	}    	
    }
}
