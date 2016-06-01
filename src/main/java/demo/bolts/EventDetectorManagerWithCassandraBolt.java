package demo.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import demo.bolts.RoundInfo;
import demo.cass.CassandraDao;

import java.util.*;

public class EventDetectorManagerWithCassandraBolt extends BaseRichBolt {

  private OutputCollector collector;
  private HashMap<Long, RoundInfo> roundInfoList;
  CassandraDao cassandraDao;

  private ArrayList<String> documents;
  private UUID id = UUID.randomUUID();


  public EventDetectorManagerWithCassandraBolt(CassandraDao cassandraDao)
  {
    this.cassandraDao = cassandraDao;
  }

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
          insertValuesToCass(round, w.getKey(), country, w.getValue());
          this.collector.emit(new Values(rounds, w.getKey(), "word", round, source, country));
        }
      }

      for(Map.Entry<String,Long> h : roundInfo.getHashtagCounts().entrySet())
      {
        if(h != null) {
          insertValuesToCass(round, h.getKey(), country, h.getValue());
          this.collector.emit(new Values(rounds, h.getKey(), "hashtag", round, source, country));
        }
      }
    }
  }

  private void insertValuesToCass(long round, String word, String country, long count)
  {
    try {
      List<Object> values = new ArrayList<>();
      values.add(round);
      values.add(word);
      values.add(country);
      values.add(count);
      values.add(-1L);
      cassandraDao.insertIntoCounts(values.toArray());
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
    declarer.declare(new Fields("rounds", "key", "type", "round", "source", "country"));
  }

}
