package eventDetector.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cassandraConnector.CassandraDao;

import java.util.*;

public class EventDetectorManagerWithCassandraBolt extends BaseRichBolt {

  private OutputCollector collector;
  private HashMap<Long, RoundInfo> roundInfoListUSA;
  private HashMap<Long, RoundInfo> roundInfoListCAN;
  private CassandraDao cassandraDao;

  public EventDetectorManagerWithCassandraBolt(CassandraDao cassandraDao)
  {
    this.cassandraDao = cassandraDao;
  }

  @Override
  public void prepare(Map config, TopologyContext context,
                      OutputCollector collector) {
    this.collector = collector;
    roundInfoListUSA = new HashMap<>();
    roundInfoListCAN = new HashMap<>();
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
    HashMap<Long, RoundInfo> roundInfoListTmp;
    if(country.equals("USA"))
      roundInfoListTmp = roundInfoListUSA;
    else
      roundInfoListTmp = roundInfoListCAN;

    if(roundInfoListTmp.get(round) != null)
    {
      roundInfo = roundInfoListTmp.get(round);
    }
    else
    {
      roundInfo = new RoundInfo();
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
    }
    roundInfoListTmp.put(round, roundInfo);

    if(country.equals("USA"))
      roundInfoListUSA = roundInfoListTmp;
    else
      roundInfoListCAN = roundInfoListTmp;

    if(roundInfo.isEndOfRound())
    {
      ArrayList<Long> rounds = (ArrayList<Long>)tuple.getValueByField("dates");

      for(Map.Entry<String,Long> w : roundInfoListUSA.get(round).getWordCounts().entrySet())
      {
        if(w != null) {
          insertValuesToCass(round, w.getKey(), "USA", w.getValue());
          this.collector.emit(new Values(rounds, w.getKey(), "word", round, source, "USA"));
        }
      }

      for(Map.Entry<String,Long> h : roundInfoListUSA.get(round).getHashtagCounts().entrySet())
      {
        if(h != null) {
          insertValuesToCass(round, h.getKey(), "USA", h.getValue());
          this.collector.emit(new Values(rounds, h.getKey(), "hashtag", round, source, "USA"));
        }
      }

      for(Map.Entry<String,Long> w : roundInfoListCAN.get(round).getWordCounts().entrySet())
      {
        if(w != null) {
          insertValuesToCass(round, w.getKey(), "CAN", w.getValue());
          this.collector.emit(new Values(rounds, w.getKey(), "word", round, source, "CAN"));
        }
      }

      for(Map.Entry<String,Long> h : roundInfoListCAN.get(round).getHashtagCounts().entrySet())
      {
        if(h != null) {
          insertValuesToCass(round, h.getKey(), "CAN", h.getValue());
          this.collector.emit(new Values(rounds, h.getKey(), "hashtag", round, source, "CAN"));
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
