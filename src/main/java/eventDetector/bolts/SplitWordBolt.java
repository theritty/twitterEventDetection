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

        if(!countryX.equals(country)) return ;

        List<String> tweets;
        long round = tuple.getLongByField("round");
        String source = (String) tuple.getValueByField( "source" );
        Boolean blockEnd = (Boolean) tuple.getValueByField("blockEnd");

        if(source.equals("twitter"))
        {
            tweets = (List<String>) tuple.getValueByField( "tweet" );
        }
        else
        {
            tweets = Arrays.asList(((String) tuple.getValueByField("tweet")).split(" "));
        }

        System.out.println("Split Word: " + tweets + " " + round);
        for(String tweet: tweets)
        {

            if(!tweet.startsWith("#") && !tweet.equals("") && tweet.length()>2
                    && !tweet.equals("hiring") && !tweet.equals("careerarc") && !tweet.equals("BLOCKEND"))
            {
                System.out.println("Split Word: " + tweet + " round:::::::::::::..." + round);
                this.collector.emit(new Values(tweet, "WordCount", round, source,
                        false, tuple.getValueByField("dates"), country));
            }
        }
        if(blockEnd)
        {
            this.collector.emit(new Values("BLOCKEND", "WordCount", round, source,
                    true, tuple.getValueByField("dates"), country));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("word", "inputBolt", "round", "source", "blockEnd", "dates", "country"));
    }
}
