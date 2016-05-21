package topologies;


import topologies.topologyBuild.TopologyCreator;

public class EventDetectionTopology {
    public static void main(String[] args) throws Exception {
        TopologyCreator topologyCreator = new TopologyCreator();
        topologyCreator.submitTopology();
    }
}