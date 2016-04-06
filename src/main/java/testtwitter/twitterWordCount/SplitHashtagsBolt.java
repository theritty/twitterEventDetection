package testtwitter.twitterWordCount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import scala.util.parsing.combinator.testing.Str;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.util.Map;

/**
 * Created by ceren on 08.03.2016.
 */
public class SplitHashtagsBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        Status tweet = (Status) tuple.getValueByField( "tweet" );

        HashtagEntity[] hashtags = tweet.getHashtagEntities();

        for(HashtagEntity hashtagEntity : hashtags){
            String word_prev = hashtagEntity.getText();
            String word = word_prev.replaceAll(
                    "[^A-Za-z0-9 _.,;:@^#+*=?&%£é\\{\\}\\(\\)\\[\\]<>|\\-$!\\\"'\\/$ığüşöçİÜĞÇÖŞ]*", "");

            if(!word.equals("") && word.length()>4)
                this.collector.emit(new Values(word, "HashtagCount"));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("word", "inputBolt"));
    }
}
