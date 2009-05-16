package mapreducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

public class JobClient extends Thread
{
    // Pass down mrHandler
    MRProtocolHandler mrHandler;
    public int nodeID;
    
    boolean allowJobClientThreadToRun = false;
	ArrayList<String> words;
    
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
        String wordToSearch = ConfigSettings.wordToSearch;
        
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
                    
                	String text;
                    
                    //Construct file in arraylist
            		try 
            		{
    
            			words = new ArrayList<String>();
            			FileReader fileReader = new FileReader(ConfigSettings.workTextFile);
            			BufferedReader reader = new BufferedReader(fileReader);

            	    	while ((text = reader.readLine()) != null)
            	    	{
            	    		String[] temp = text.split(" ");
            	    
            	    		//Add each word to a list for splitting up into chunks later
            	    		for (int j = 0; j < temp.length; j++)
            	    			words.add(temp[j]);
            	    	}
            	    	
            	    	//add a padding word
            	    	words.add("");
            	    	
            	    	reader.close();
            	    	fileReader.close();
            		} 
            		catch (Exception e) 
            		{
            			e.printStackTrace();
            		}    	  
                    
                    
                    
                    //submit MR job
                    mrHandler.SubmitMRJob(fileName, wordToSearch);
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
    public void getDataset(int beginIndex, int endIndex, int workerNodeDest)
    {
        // TODO Get the dataset and send back to the requesting worker using
        // the MR_JOB_DATASET_RESULT message
    	  	
    	ArrayList<String> words = new ArrayList<String>();
    	String text;
    	String returnData = "";
    	    	
		try 
		{
			FileReader fileReader = new FileReader(ConfigSettings.workTextFile);
			BufferedReader reader = new BufferedReader(fileReader);

	    	while ((text = reader.readLine()) != null)
	    	{
	    		String[] temp = text.split(" ");
	    
	    		//Add each word to a list for splitting up into chunks later
	    		for (int j = 0; j < temp.length; j++)
	    			words.add(temp[j]);
	    	}
	    	
	    	//add a padding word
	    	words.add("");
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}    	
    	
		//Get the slice for the worker.  Just return it as a string for now
    	for (int j = beginIndex; j <= endIndex; j++)
    		returnData = returnData + words.get(j) + " ";
       	
    	//Send it out
    	this.mrHandler.JobClientSendData(returnData, workerNodeDest);
   	
    }

    /*
     * Method to handle PeerNodeMessageType.MR_JOB_REDUCE_RESULT
     * 
     * The results of the various map/reduce results are handled here. This is
     * where the merge will be done.
     */
    public void reduceResult(ArrayList<Integer> results)
    {
        // TODO Method which calculates result of all data
    	int total = 0;
    	
    	for (int i = 0; i < results.size(); i++)
    		total = total + results.get(i);
    	
    	System.out.println("Job Client Received Worker Results - Answer: " + total);
    }
}
