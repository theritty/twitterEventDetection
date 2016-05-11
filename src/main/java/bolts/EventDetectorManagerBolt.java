package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;

public class EventDetectorManagerBolt extends BaseRichBolt {

    private OutputCollector collector;
    private HashMap<String,Long> wordcount;
    private HashMap<String,Long> hashtagcount;
    private ArrayList<String> documents;
    private int threshold;
    private UUID id = UUID.randomUUID();

    public EventDetectorManagerBolt(int threshold)
    {
        this.threshold = threshold;
    }
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
        String source = (String) tuple.getValueByField( "source" );
        if(inputBolt.equals("WordCount"))
        {
            String word = tuple.getStringByField("word");
            Long count = tuple.getLongByField("count");
            if(count >= threshold) {
                wordcount.put(word, count);
            }
//            System.out.println("word count for " + word + " " + count + " id " + id);
        }
        else if(inputBolt.equals("HashtagCount"))
        {
            String word = tuple.getStringByField("word");
            Long count = tuple.getLongByField("count");
            if(count >= threshold) {
                hashtagcount.put(word, count);
            }
//            System.out.println("word count for " + word + " " + count + " id " + id);
        }
        else //Document creator bolt
        {
            Boolean blockEnd = (Boolean) tuple.getValueByField("blockEnd");

            if(blockEnd)
            {
                System.out.println(", and it is blockend.. " + " id " + id);
                ArrayList<Date> dates = (ArrayList<Date>)tuple.getValueByField("dates");
                long round = tuple.getLongByField("round");
                ArrayList<String> dateList = new ArrayList<>();

                for (Date date : dates)
                {
                    dateList.add(date.toString() + ".txt");
                }

                ArrayList<String> words = new ArrayList<>();
                ArrayList<String> hashtags = new ArrayList<>();

                prepareLists(words, hashtags);


                System.out.println("Lists are from manager Word:  " + words.toString());
                System.out.println("Lists are from manager Hash:  " + hashtags.toString());

                wordcount.clear();
                hashtagcount.clear();


                System.out.println("manager::: dates " + dates + " datelist " + dateList);

                for(String word : words)
                {
                    if(word != null) {
                        this.collector.emit(new Values(dateList, word, "word", round, source));
                    }
                }

                for(String hashtag : hashtags)
                {
                    if(hashtag != null) {
                        this.collector.emit(new Values(dateList, hashtag, "hashtag", round, source));
                    }
                }
            }
            else
            {

//                System.out.println(", not blockend.. ");
            }


        }


    }

    private void prepareLists(ArrayList<String> words, ArrayList<String> hashtags)
    {
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
            if(e.getValue() > threshold)
            {
                words.add(e.getKey());
            }
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
            if(e.getValue() > threshold)
            {
                hashtags.add(e.getKey());
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("dates", "key", "type", "round", "source"));
    }
}
