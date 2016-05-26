package bolts.CassandraSpoutProcessing;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import bolts.CommonBolts.RoundInfo;

import java.util.*;

public class EventDetectorManagerWithCassandraBolt extends BaseRichBolt {

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
      ArrayList<Long> rounds = (ArrayList<Long>)tuple.getValueByField("dates");
//            System.out.println("MNG - Blockend:: Round " + round + " word " + word
//                    + " count " + count + ", rounds " + rounds+
//                    " roundtrack " + roundInfo.getRoundCheckInBits() );

//            ArrayList<String> words = new ArrayList<>();
//            ArrayList<String> hashtags = new ArrayList<>();

//            prepareLists(roundInfo, words, hashtags, round);


      System.out.println("Round " + round + " is done. " +
                "\n Lists are from manager Word:  " +
                  roundInfo.getWordCounts().entrySet().toString() +
                "\n Lists are from manager Hash:  "  +
                  roundInfo.getHashtagCounts().entrySet().toString() +
               "\n Rounds are from manager: " + rounds );

      for(Map.Entry<String,Long> w : roundInfo.getWordCounts().entrySet())
      {
        if(w != null) {
          this.collector.emit(new Values(rounds, w.getKey(), "word", round, source, country));
        }
      }

      for(Map.Entry<String,Long> h : roundInfo.getHashtagCounts().entrySet())
      {
        if(h != null) {
          this.collector.emit(new Values(rounds, h.getKey(), "hashtag", round, source, country));
        }
      }
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    declarer.declare(new Fields("rounds", "key", "type", "round", "source", "country"));
  }

}
