package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.List;
import java.util.Map;

public class SplitWordBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
//        Status tweet = (Status) tuple.getValueByField( "tweet" );
        List<String> tweets = (List<String>) tuple.getValueByField( "tweet" );
        long round = tuple.getLongByField("round");

//        String[] words = sentence_preprocessed.split(" ");
//        for(String word : words){
//            String wordAfterNlp = stemWord(word);
//            if(!wordAfterNlp.equals("") && wordAfterNlp.length()>4 && !wordAfterNlp.startsWith("#"))
//                this.collector.emit(new Values(wordAfterNlp, "WordCount", round));
//        }

        for(String tweet: tweets)
        {
            if(!tweet.startsWith("#") && !tweet.equals("") && tweet.length()>4
                    && !tweet.equals("hiring") && !tweet.equals("careerarc"))
            {
                this.collector.emit(new Values(tweet, "WordCount", round));
            }
        }

    }

//    public String removeUnnecessary(String sentence)
//    {
//        String sentence_processed = sentence.replaceAll("[^A-Za-z0-9 _.,;:@^#+*=?&%£é\\{\\}\\(\\)\\[\\]<>|\\-$!\\\"'\\/$ığüşöçİÜĞÇÖŞ]*", "");
//        return sentence_processed;
//
//    }
//    public String endWordElimination(String sentence)
//    {
//        return sentence;
//    }
//
//    public String stemWord(String word)
//    {
//        return word;
//    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("word", "inputBolt", "round"));
    }
}
