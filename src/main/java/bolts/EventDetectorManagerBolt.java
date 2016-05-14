package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

public class EventDetectorManagerBolt extends BaseRichBolt {

    private OutputCollector collector;
    private HashMap<Long, RoundInfo> roundInfoList;

    private ArrayList<String> documents;
    private UUID id = UUID.randomUUID();


    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        documents = new ArrayList<>();
        roundInfoList = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String inputBolt = tuple.getStringByField( "inputBolt" );
        String source = (String) tuple.getValueByField( "source" );
        long round = tuple.getLongByField("round");
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        Boolean blockEnd = (Boolean) tuple.getValueByField("blockEnd");

        RoundInfo roundInfo;
        if(roundInfoList.get(round) != null)
        {
            roundInfo = roundInfoList.get(round);
        }
        else
        {
            roundInfo = new RoundInfo();
            roundInfoList.put(round, roundInfo);
        }

        if(inputBolt.equals("WordCount"))
        {
            if(blockEnd)
            {
                roundInfo.setWordBlockEnd();
            }
            else
            {
                roundInfo.putWord(word, count);
            }

            System.out.println("MNG - WordCount:: Round " + round + ", word " + word+ ", count " + count +
                    " roundtrack " + roundInfo.getRoundCheckInBits() + " blockend: " + blockEnd);
        }
        else if(inputBolt.equals("HashtagCount"))
        {
            if(blockEnd)
            {
                roundInfo.setHashtagBlockEnd();
            }
            else
            {
                roundInfo.putHashtag(word, count);
            }

            System.out.println("MNG - HashtagCount:: Round " + round + ", word " + word+ ", count " + count +
                    " roundtrack " + roundInfo.getRoundCheckInBits() + " blockend: " + blockEnd );
        }


        if(roundInfo.isEndOfRound())
        {
            ArrayList<Date> dates = (ArrayList<Date>)tuple.getValueByField("dates");
            System.out.println("MNG - Blockend:: Round " + round + " word " + word
                    + " count " + count + ", dates " + dates+
                    " roundtrack " + roundInfo.getRoundCheckInBits() );
            ArrayList<String> dateList = new ArrayList<>();

            for (Date date : dates)
            {
                dateList.add(date.toString() + ".txt");
            }

            ArrayList<String> words = new ArrayList<>();
            ArrayList<String> hashtags = new ArrayList<>();

            prepareLists(roundInfo, words, hashtags, round);


            System.out.println("Lists are from manager Word:  " + words.toString());
            System.out.println("Lists are from manager Hash:  " + hashtags.toString());
            System.out.println("manager::: dates " + dates + " datelist " + dateList);

            for(String w : words)
            {
                if(w != null) {
                    this.collector.emit(new Values(dateList, w, "word", round, source));
                }
            }

            for(String h : hashtags)
            {
                if(h != null) {
                    this.collector.emit(new Values(dateList, h, "hashtag", round, source));
                }
            }
        }
    }

    private void prepareLists(RoundInfo roundInfo, ArrayList<String> words, ArrayList<String> hashtags, long round)
    {
        List<Map.Entry<String,Long>> entries = new ArrayList<>(
                roundInfo.getWordCounts().entrySet()
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
                words.add(e.getKey());
        }


        List<Map.Entry<String,Long>> entries2 = new ArrayList<>(
                roundInfo.getHashtagCounts().entrySet()
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
                hashtags.add(e.getKey());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("dates", "key", "type", "round", "source"));
    }

}
