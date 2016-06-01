package demo.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SplitHashtagsBolt extends BaseRichBolt {

    private OutputCollector collector;
    private String country;
    static Logger log = LoggerFactory.getLogger(SplitHashtagsBolt.class);

    public SplitHashtagsBolt(String country)
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

//        System.out.println("Blockend is " + blockEnd);
        if(source.equals("twitter"))
        {
            tweets = (List<String>) tuple.getValueByField( "tweet" );
        }
        else
        {
            tweets = Arrays.asList(((String) tuple.getValueByField("tweet")).split(" "));
//            System.out.println("split hashtag " + tweets);
        }

        for(String tweet: tweets)
        {
            log.debug("Word: " + tweet + " round: " + round + " country: " + country);
            if(tweet.startsWith("#") && !tweet.equals("") && tweet.length()>3
                    && !tweet.equals("#hiring") && !tweet.equals("#careerarc") && !tweet.equals("BLOCKEND"))
            {
                this.collector.emit(new Values(tweet/*.replace("#", "")*/, "HashtagCount", round,
                        source, false, tuple.getValueByField("dates"), country));
            }
        }
        if(blockEnd)
        {
            log.debug("Blockend for round: " + round + " country: " + country);
            this.collector.emit(new Values("BLOCKEND", "HashtagCount", round, source, true,
                    tuple.getValueByField("dates"), country));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("word", "inputBolt", "round", "source", "blockEnd", "dates", "country"));
    }
}
