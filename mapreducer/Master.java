package mapreducer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

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

    	String text;
    	int totalWords = 0;
    	
    	System.out.println("Master::workerSubmittedJob : " + srcFile);
    	
    	//Maybe read the number of words in the file and split among workers for now
    	
		try 
		{
			FileReader fileReader = new FileReader(srcFile);
			BufferedReader reader = new BufferedReader(fileReader);

	    	while ((text = reader.readLine()) != null)
	    		totalWords = totalWords + text.split(" ").length;
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}

    	System.out.println("Total Words In File: " + totalWords + " To Split Among (" + mrHandler.getWorkerCount() + ") Workers");
    	
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
