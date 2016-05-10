package topologies;


import topologies.topologyBuild.TopologyCreator;

public class WordCountTopology {
    public WordCountTopology( )
    {

    }

    public static void main(String[] args) throws Exception {
        TopologyCreator topologyCreator = new TopologyCreator();
        topologyCreator.submitTopology();
    }
}