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

public class SplitHashtagsBolt extends BaseRichBolt {

    private OutputCollector collector;

    public SplitHashtagsBolt(String country)
    {

    }

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String country = (String) tuple.getValueByField( "country" );
        Boolean blockEnd = (Boolean) tuple.getValueByField("blockEnd");
        long round = tuple.getLongByField("round");
        String source = (String) tuple.getValueByField( "source" );

        if(blockEnd)
        {
            System.out.println("Sending blockend from splithashtag: at round " + round + " country " + country);
            this.collector.emit(new Values("BLOCKEND", "HashtagCount", round, source, true,
                    tuple.getValueByField("dates"), country));
            return;
        }

        else{
            List<String> tweets;
            if (source.equals("twitter")) {
                tweets = (List<String>) tuple.getValueByField("tweet");
            } else {
                tweets = Arrays.asList(((String) tuple.getValueByField("tweet")).split(" "));
            }
            System.out.println("Splitting Hashtag: " + tweets + " at round " + round + " country " + country);
            for (String tweet : tweets) {
                if (tweet.startsWith("#") && !tweet.equals("") && tweet.length() > 3
                        && !tweet.equals("#hiring") && !tweet.equals("#careerarc") && !tweet.equals("BLOCKEND")) {
                    System.out.println("Sending hashtag from splithashtag: " + tweet + " at round " + round + " country " + country);
                    this.collector.emit(new Values(tweet/*.replace("#", "")*/, "HashtagCount", round,
                            source, false, tuple.getValueByField("dates"), country));
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
