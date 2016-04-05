package testtwitter.twitterWordCount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import java.util.*;

public class EventDetectorBolt extends BaseRichBolt {

    private OutputCollector collector;
    private HashMap<String,Long> wordcount;
    private HashMap<String,Long> hashtagcount;
    private ArrayList<String> documents;

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        wordcount = new HashMap<>();
        hashtagcount = new HashMap<>();
        documents = new ArrayList<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String inputBolt = tuple.getStringByField( "inputBolt" );
        if(inputBolt.equals("WordCount"))
        {
            String word = tuple.getStringByField("word");
            Long count = tuple.getLongByField("count");
            wordcount.put(word,count);
        }
        else if(inputBolt.equals("HashtagCount"))
        {
            String word = tuple.getStringByField("word");
            Long count = tuple.getLongByField("count");
            hashtagcount.put(word,count);
        }
        else //Document creator bolt
        {
            Boolean blockEnd = (Boolean) tuple.getValueByField("blockEnd");

            if(blockEnd)
            {
                ArrayList<Date> dates = (ArrayList<Date>)tuple.getValueByField("dates");



                List<Map.Entry<String,Long>> entries = new ArrayList<>(
                        wordcount.entrySet()
                );
                Collections.sort(
                        entries
                        ,   new Comparator<Map.Entry<String,Long>>() {
                            public int compare(Map.Entry<String,Long> a, Map.Entry<String,Long> b) {
                                return Long.compare(b.getValue(), a.getValue());
                            }
                        }
                );
                for (Map.Entry<String,Long> e : entries) {
//                    write(writer, e.getKey() + ":" + e.getValue());
                }






                List<Map.Entry<String,Long>> entries2 = new ArrayList<>(
                        hashtagcount.entrySet()
                );
                Collections.sort(
                        entries2
                        ,   new Comparator<Map.Entry<String,Long>>() {
                            public int compare(Map.Entry<String,Long> a, Map.Entry<String,Long> b) {
                                return Long.compare(b.getValue(), a.getValue());
                            }
                        }
                );
                for (Map.Entry<String,Long> e : entries2) {
//                    write(writer, e.getKey() + ":" + e.getValue());
                }


            }
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("word"));
    }
}
