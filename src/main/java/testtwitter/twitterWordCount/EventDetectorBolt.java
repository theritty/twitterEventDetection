package testtwitter.twitterWordCount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.util.Map;

public class EventDetectorBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String inputBolt = tuple.getStringByField( "inputBolt" );
        if(inputBolt.equals("WordCount"))
        {

        }
        else if(inputBolt.equals("HashtagCount"))
        {

        }
        else //Document creator bolt
        {

        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("word"));
    }
}
