package topologies;

import topologies.topologyBuild.TopologyCreator;

public class TwitterStreamTopology {
    public static void main(String[] args) throws Exception {
        TopologyCreator topologyCreator = new TopologyCreator();
        topologyCreator.submitTwitterStreamTopology();
    }
}