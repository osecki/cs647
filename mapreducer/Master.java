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
    
    //job table which has key=workerNodeID;  value=whether they have responded with
    //answer to task.
    
    //Hash table to maintain jobs
    //Key = jobID
    //Value = hashtable (key = WorkerID, value = result)
    public Hashtable<Integer, Hashtable<Integer, Integer>> jobTable;
    
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
    public void workerSubmittedJob(String srcFile, String wordToSearch, int jobClientID)
    {
    	//job table to keep track of jobID and workerIDs and results
    	jobTable = new Hashtable<Integer, Hashtable<Integer, Integer>>();
    	
    	
        // Worker has submitted the job, now the master must divide up the work
        // TODO Put algorithm here to decide how to divide up work
    	
    	ArrayList<String> words = new ArrayList<String>();
    	String text;
    	
    	System.out.println("Master::workerSubmittedJob : " + srcFile);
    	
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
		
		//create new entry in job table
		this.jobTable.put(jobID, new Hashtable<Integer, Integer>());
		
    	//Tell workers to get the data sets
    	this.mrHandler.AssignWorkersJob(jobID, words, wordToSearch, jobClientID);
    }

    /*
     * Method to handle PeerNodeMessageType.MR_JOB_COMPLETE
     * 
     * Each worker sends the MR_JOB_COMPLETE message to the master. When all
     * workers have sent the JOB COMPLETE for a particular job, then notify the
     * Job Client
     */
    public void jobComplete(int jobID, int workerID, int result)
    {
    	boolean done = true;
    	
    	System.out.println("Master::jobComplete Job " + jobID + " - " + "Worker " + workerID + " finished with results: " + result);

    	//Update job table with result
    	jobTable.get(jobID).put(workerID, result);
    	
    	//check to see if all workers have answered
    	Iterator<Integer> iter = jobTable.get(jobID).keySet().iterator();
    	while (iter.hasNext())
    	{
    		int workerResult = jobTable.get(jobID).get(iter.next());
    		if (workerResult == -1)
    		{
    			done = false;
    		}
    	}
    	
        // Tell the job client the work is complete
    	if (done)
    	{
    		mrHandler.WorkComplete();
    	}

    }
}
