package mapreducer;

public class Simulator implements Runnable
{
    public EventLogging eventLogger;
    public StatisticsLogging statLogger;
    public P2PCommsManager[] p2pComms;
    public MRProtocolHandler[] mrHandler;
    public Master[] master;
    public Worker[] worker;
    public JobClient[] jobClient;
    public FaultAndHealth[] faultHealth;
    public ConfigSettings config;

    public Simulator()
    {
        // TODO: Instantiate everything, pass references, etc

        // TODO: Start all threads

    }

    public void run()
    {
        // TODO: Simulator Thread
    }

}
