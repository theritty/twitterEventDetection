package testtwitter.twitterWordCount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import nlp.TextAnalyzer;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by ceren on 28.04.2016.
 */
public class PreprocessTweet extends BaseRichBolt {

    private TextAnalyzer textAnalyzer;
    private OutputCollector collector;

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        textAnalyzer = new TextAnalyzer();
    }

    //new Values(ret, dates, currentDate, false, round)
    //new Fields("tweet", "dates","currentDate","blockEnd", "round")
    @Override
    public void execute(Tuple tuple) {
        Status tweet = (Status) tuple.getValueByField( "tweet" );

        List<String> preprocessText = textAnalyzer.extractWordList(tweet.getText());
        this.collector.emit(new Values(preprocessText,
                tuple.getValueByField( "dates" ),
                tuple.getValueByField( "currentDate" ),
                tuple.getValueByField( "blockEnd" ),
                tuple.getLongByField("round")));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("tweet","dates","currentDate","blockEnd", "round"));
    }
}
