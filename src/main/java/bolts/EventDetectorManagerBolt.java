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

  private HashMap<Long, ArrayList<HashMap>> countList;
  private HashMap<Long, ArrayList<Date>> dateHash;
  private HashMap<Long,Integer> roundTrack;
  private ArrayList<String> documents;
  private int threshold;
  private int currentRound=-1;
  private UUID id = UUID.randomUUID();

  public EventDetectorManagerBolt(int threshold)
  {
    this.threshold = threshold;
  }
  @Override
  public void prepare(Map config, TopologyContext context,
                      OutputCollector collector) {
    this.collector = collector;
    documents = new ArrayList<>();
    roundTrack = new HashMap<>();
    countList = new HashMap<>();
    dateHash = new HashMap<>();
  }

  @Override
  public void execute(Tuple tuple) {
    String inputBolt = tuple.getStringByField( "inputBolt" );
    String source = (String) tuple.getValueByField( "source" );
    long round = tuple.getLongByField("round");
    int rndCnt = 0;
    if(roundTrack.get(round) != null)
    {
      rndCnt = roundTrack.get(round);
    }
    if(currentRound < round)
    {
      HashMap<String,Long> wordcount;
      HashMap<String,Long> hashtagcount;

      wordcount = new HashMap<>();
      hashtagcount = new HashMap<>();
      ArrayList<HashMap> hashMapArrayList = new ArrayList<>();
      hashMapArrayList.add(wordcount);
      hashMapArrayList.add(hashtagcount);

      countList.put(round, hashMapArrayList);
      roundTrack.put(round,0);
    }
    if(inputBolt.equals("WordCount"))
    {
      String word = tuple.getStringByField("word");
      Long count = tuple.getLongByField("count");

      roundTrack.put(round, rndCnt | 1);

      if(count >= threshold) {
        countList.get(round).get(0).put(word, count);
      }
//            System.out.println("word count for " + word + " " + count + " id " + id);
    }
    else if(inputBolt.equals("HashtagCount"))
    {
      String word = tuple.getStringByField("word");
      Long count = tuple.getLongByField("count");

      roundTrack.put(round, rndCnt | 2);

      if(count >= threshold) {
        countList.get(round).get(1).put(word, count);
      }
//            System.out.println("word count for " + word + " " + count + " id " + id);
    }
    else //Document creator bolt
    {
      Boolean blockEnd = (Boolean) tuple.getValueByField("blockEnd");

      if(blockEnd)
      {
        ArrayList<Date> dates = (ArrayList<Date>)tuple.getValueByField("dates");
        roundTrack.put(round, rndCnt | 4);
        dateHash.put(round, dates);
      }

    }

    if(roundTrack.get(round) == 7)
    {
      System.out.println(", and it is blockend.. " + " id " + id);
      ArrayList<String> dateList = new ArrayList<>();

      for (Date date : dateHash.get(round))
      {
        dateList.add(date.toString() + ".txt");
      }

      ArrayList<String> words = new ArrayList<>();
      ArrayList<String> hashtags = new ArrayList<>();

      prepareLists(words, hashtags, round);


      System.out.println("Lists are from manager Word:  " + words.toString());
      System.out.println("Lists are from manager Hash:  " + hashtags.toString());

      countList.get(round).clear();

      System.out.println("manager::: dates " + dateHash.get(round) + " datelist " + dateList);

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
  }

  private void prepareLists(ArrayList<String> words, ArrayList<String> hashtags, long round)
  {
    List<Map.Entry<String,Long>> entries = new ArrayList<>(
            countList.get(round).get(0).entrySet()
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
            countList.get(round).get(1).entrySet()
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
