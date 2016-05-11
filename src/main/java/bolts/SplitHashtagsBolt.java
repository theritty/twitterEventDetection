package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SplitHashtagsBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        List<String> tweets;
        long round = tuple.getLongByField("round");
        String source = (String) tuple.getValueByField( "source" );

        if(source.equals("twitter"))
        {
            tweets = (List<String>) tuple.getValueByField( "tweet" );
        }
        else
        {
            tweets = Arrays.asList(((String) tuple.getValueByField("tweet")).split(" "));
        }

        for(String tweet: tweets)
        {
            if(tweet.startsWith("#") && !tweet.equals("") && tweet.length()>4
                    && !tweet.equals("#hiring") && !tweet.equals("#careerarc"))
            {
                this.collector.emit(new Values(tweet.replace("#", ""), "HashtagCount", round, source));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("word", "inputBolt", "round", "source"));
    }
}
