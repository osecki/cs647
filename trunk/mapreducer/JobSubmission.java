package mapreducer;

public class JobSubmission 
{	
	public int jobID;
	public int jobClientID;
	public int workerNodeID;
	public int dataChunkID;
	public int dataSetBlockNumBeginIndex;	
	public int dataSetBlockNumEndIndex;
	public byte[] dataset;
	public int result;
}
