package spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import cass.CassandraDao;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import java.util.*;

public class CassandraSpout extends BaseRichSpout {

  private SpoutOutputCollector collector;
  CassandraDao cassandraDao;
  boolean readed=false;
  ArrayList<Long> roundlist;
  ArrayList<Long> readRoundlist;
  int compareSize;
  int trainSize;

  public CassandraSpout(CassandraDao cassandraDao, int trainSize, int compareSize)
  {
    this.cassandraDao = cassandraDao;
    this.compareSize = compareSize;
    this.trainSize = trainSize;
    roundlist = new ArrayList<>();
    readRoundlist = new ArrayList<>();
  }
  @Override
  public void ack(Object msgId) {}
  @Override
  public void close() {}

  @Override
  public void fail(Object msgId) {}

  /**
   * The only thing that the methods will do It is emit each
   * file line
   */
  @Override
  public void nextTuple() {
    /**
     * The nextuple it is called forever, so if we have been readed the file
     * we will wait and then return
     */
    if(readed){
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        //Do nothing
      }
      return;
    }

    for(long round : roundlist) {
      readRoundlist.add(round);

      if (readRoundlist.size() > compareSize) readRoundlist.remove(0);
      if (readRoundlist.size() <= trainSize) continue;



      ResultSet resultSet = getDataFromCassandra(round);

      Iterator<Row> iterator = resultSet.iterator();
      while (iterator.hasNext()) {
        Row row = iterator.next();
        String tweet = row.getString("tweet");
        String country = row.getString("country");

        if(tweet == null || tweet.length() == 0) continue;
        ArrayList<Long> tmp_roundlist = new ArrayList<>(readRoundlist);
//        System.out.println("Tweet: " + tweet);
        if(iterator.hasNext())
          collector.emit(new Values(tweet, tmp_roundlist, round, false, round, "cassandra", "cassandraSpout", country));
        else
          collector.emit(new Values(tweet, tmp_roundlist, round, true, round, "cassandra", "cassandraSpout", country));
      }
    }

    readed = true;
    System.out.println("Reading finished.");

  }

  public void getRoundListFromCassandra(){
    ResultSet resultSet = cassandraDao.readRules("SELECT DISTINCT round FROM tweets3;");
    roundlist = new ArrayList<>();

    Iterator<Row> iterator = resultSet.iterator();
    while(iterator.hasNext())
    {
      Row row = iterator.next();
      roundlist.add(row.getLong("round"));
    }
    Collections.sort(roundlist, new Comparator<Long>() {
      public int compare(Long m1, Long m2) {
        return m1.compareTo(m2);
      }
    });

  }


  public ResultSet getDataFromCassandra(long round) {
    ResultSet resultSet = cassandraDao.readRules("SELECT tweet,country FROM tweets3 WHERE round=" + round + ";");

    return resultSet;
  }

  /**
   * We will create the file and get the collector object
   */
  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {
    getRoundListFromCassandra();
    this.collector = collector;
  }

  /**
   * Declare the output field "word"
   */
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("tweet", "dates","currentDate","blockEnd", "round", "source", "inputBolt", "country"));
  }

}