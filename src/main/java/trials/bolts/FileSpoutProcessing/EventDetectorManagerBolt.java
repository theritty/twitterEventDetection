package trials.bolts.FileSpoutProcessing;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import trials.bolts.CommonBolts.RoundInfo;

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
    String country = (String) tuple.getValueByField( "country" );
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
    if(roundInfo.isEndOfRound()) return;

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

//            System.out.println("MNG - WordCount:: Round " + round + ", word " + word+ ", count " + count +
//                    " roundtrack " + roundInfo.getRoundCheckInBits() + " blockend: " + blockEnd);
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

//            System.out.println("MNG - HashtagCount:: Round " + round + ", word " + word+ ", count " + count +
//                    " roundtrack " + roundInfo.getRoundCheckInBits() + " blockend: " + blockEnd );
    }


    if(roundInfo.isEndOfRound())
    {
      ArrayList<Date> dates = (ArrayList<Date>)tuple.getValueByField("dates");
//            System.out.println("MNG - Blockend:: Round " + round + " word " + word
//                    + " count " + count + ", dates " + dates+
//                    " roundtrack " + roundInfo.getRoundCheckInBits() );
      ArrayList<String> dateList = new ArrayList<>();

      for (Date date : dates)
      {
        dateList.add(date.toString() + ".txt");
      }

//            ArrayList<String> words = new ArrayList<>();
//            ArrayList<String> hashtags = new ArrayList<>();

//            prepareLists(roundInfo, words, hashtags, round);


      System.out.println("Round " + round + " is done. " +
                "\n Lists are from manager Word:  " +
                  roundInfo.getWordCounts().entrySet().toString() +
                "\n Lists are from manager Hash:  "  +
                  roundInfo.getHashtagCounts().entrySet().toString() +
               "\n Dates are from manager: " + dates + " datelist " + dateList);

      for(Map.Entry<String,Long> w : roundInfo.getWordCounts().entrySet())
      {
        if(w != null) {
          this.collector.emit(new Values(dateList, w.getKey(), "word", round, source, country));
        }
      }

      for(Map.Entry<String,Long> h : roundInfo.getHashtagCounts().entrySet())
      {
        if(h != null) {
          this.collector.emit(new Values(dateList, h.getKey(), "hashtag", round, source, country));
        }
      }
    }
  }

//    private void prepareLists(RoundInfo roundInfo, ArrayList<String> words, ArrayList<String> hashtags, long round)
//    {
//        List<Map.Entry<String,Long>> entries = new ArrayList<>(
//                roundInfo.getWordCounts().entrySet()
//        );
//        Collections.sort(
//                entries
//                ,   new Comparator<Map.Entry<String,Long>>() {
//                    public int compare(Map.Entry<String,Long> a, Map.Entry<String,Long> b) {
//                        return Long.compare(b.getValue(), a.getValue());
//                    }
//                }
//        );
//        for (Map.Entry<String,Long> e : entries) {
//                words.add(e.getKey());
//        }
//
//
//        List<Map.Entry<String,Long>> entries2 = new ArrayList<>(
//                roundInfo.getHashtagCounts().entrySet()
//        );
//        Collections.sort(
//                entries2
//                ,   new Comparator<Map.Entry<String,Long>>() {
//                    public int compare(Map.Entry<String,Long> a, Map.Entry<String,Long> b) {
//                        return Long.compare(b.getValue(), a.getValue());
//                    }
//                }
//        );
//        for (Map.Entry<String,Long> e : entries2) {
//                hashtags.add(e.getKey());
//        }
//    }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    declarer.declare(new Fields("dates", "key", "type", "round", "source", "country"));
  }

}
