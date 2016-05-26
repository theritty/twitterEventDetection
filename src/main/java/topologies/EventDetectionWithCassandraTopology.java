package topologies;


import topologies.topologyBuild.TopologyCreator;

public class EventDetectionWithCassandraTopology {
    public static void main(String[] args) throws Exception {
        TopologyCreator topologyCreator = new TopologyCreator();
        topologyCreator.submitTopologyWithCassandra();
    }
}