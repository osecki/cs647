package mapreducer;

public class JobClient
{
    public MRProtocolHandler mrHandler;

    public JobClient()
    {

    }

    public void SetMRProtocolHandlerRef(MRProtocolHandler reference)
    {
        mrHandler = reference;
    }

}
