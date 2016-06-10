package eventDetector.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SplitWordBolt extends BaseRichBolt {

    private OutputCollector collector;
    private String country;

    public SplitWordBolt(String country)
    {
        this.country = country;
    }

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String countryX = (String) tuple.getValueByField( "country" );
        Boolean blockEnd = (Boolean) tuple.getValueByField("blockEnd");
        String source = (String) tuple.getValueByField( "source" );
        long round = tuple.getLongByField("round");

        if(blockEnd)
        {
//            System.out.println("Sending blockend from splitword at round " + round + " country " + country);
            this.collector.emit(new Values("BLOCKEND", "WordCount", round, source,
                    true, tuple.getValueByField("dates"), country));
            return;
        }

        if(countryX.equals(country)) {

            List<String> tweets;

            if (source.equals("twitter")) {
                tweets = (List<String>) tuple.getValueByField("tweet");
            } else {
                tweets = Arrays.asList(((String) tuple.getValueByField("tweet")).split(" "));
            }

//            System.out.println("Splitting Word: " + tweets + " at round " + round + " country " + country);
            for (String tweet : tweets) {

                if (!tweet.startsWith("#") && !tweet.equals("") && tweet.length() > 2
                        && !tweet.equals("hiring") && !tweet.equals("careerarc") && !tweet.equals("BLOCKEND")) {
//                    System.out.println("Sending word from splitword: " + tweet + " at round " + round + " country " + country);
                    this.collector.emit(new Values(tweet, "WordCount", round, source,
                            false, tuple.getValueByField("dates"), country));
                }
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("word", "inputBolt", "round", "source", "blockEnd", "dates", "country"));
    }
}
