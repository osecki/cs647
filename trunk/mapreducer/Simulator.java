package mapreducer;

import java.util.Enumeration;
import java.util.Hashtable;

public class Simulator implements Runnable
{
    public EventLogging eventLogger;
    public StatisticsLogging statLogger;
    public ConfigSettings config;
    public Hashtable<Integer, PeerNodeType> peerNodes;

    public Simulator()
    {

    }

    public void run()
    {
        // Initialize and get the simulator up and running
        initializeMapReduceSimulator();

        // Now run the scenarios ??

    }

    private void initializeMapReduceSimulator()
    {
        peerNodes = new Hashtable<Integer, PeerNodeType>();

        // Create all nodes and add to hashtable
        // Initially set all nodes to workers
        for (int i = 0; i < ConfigSettings.numNodes; i++)
        {
            int id;

            PeerNodeType node = new PeerNodeType();
            id = node.hashCode();
            node.setRoleType(PeerNodeRoleType.WORKER);
            node.setNodeName();
            peerNodes.put(id, node);
        }

        // Randomly select initial job client
        selectRandomJobClient();

        // Randomly select initial job client
        selectRandomMaster();

        // Run nodes
        // This will start the communications thread and the map/reduce thread
        // in the Worker Node and the JobClient thread which discovers the
        // master node
        runNodeThreads();

        // Next, we need to notify the Worker and JobClient nodes who the master
        // node is. We need to do this now because we need to make sure all
        // nodes
        // know who the master is during the startup of the simulator. Later on,
        // this logic will be used when the Master Election logic is run and a
        // new
        // Master is elected.
        // Find the master node.
        Enumeration<PeerNodeType> e = peerNodes.elements();

        while (e.hasMoreElements())
        {
            PeerNodeType node = e.nextElement();

            if (node.getRoleType() == PeerNodeRoleType.MASTER)
            {
                node.sim_InitialMasterNodeIDBroadcast();
                break;
            }
        }

        // TODO: Are there other things to do here ???

    }

    private void selectRandomJobClient()
    {
        int jobClientIndex = (int) (0 + Math.random() * ConfigSettings.numNodes);
        Integer jobClientKey = (Integer) peerNodes.keySet().toArray()[jobClientIndex];
        peerNodes.get(jobClientKey).setRoleType(PeerNodeRoleType.CLIENT);
        peerNodes.get(jobClientKey).setNodeName();
    }

    private void selectRandomMaster()
    {
        boolean foundMaster = false;

        // Make sure that our initial master is not the client we just selected

        while (!foundMaster)
        {
            int masterClientIndex = (int) (0 + Math.random() * ConfigSettings.numNodes);
            Integer masterClientKey = (Integer) peerNodes.keySet().toArray()[masterClientIndex];

            if (peerNodes.get(masterClientKey).getRoleType() != PeerNodeRoleType.CLIENT)
            {
                peerNodes.get(masterClientKey).setRoleType(PeerNodeRoleType.MASTER);
                peerNodes.get(masterClientKey).setNodeName();
                foundMaster = true;
            }
        }
    }

    private void runNodeThreads()
    {
        // Run all node threads
        Enumeration<PeerNodeType> e = peerNodes.elements();

        while (e.hasMoreElements())
            e.nextElement().run();
    }
}
