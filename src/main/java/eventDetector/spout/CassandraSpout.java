package eventDetector.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import cassandraConnector.CassandraDao;
import eventDetector.getEventInfo;

import java.util.*;

public class CassandraSpout extends BaseRichSpout {

  private SpoutOutputCollector collector;
  private CassandraDao cassandraDao;
  private ArrayList<Long> roundlist;
  private ArrayList<Long> readRoundlist;
  private int compareSize;
  private int trainSize;
  private int testSize;
  private Iterator<Row> iterator = null;
  private int batch_size;
  private int remaining_for_batch;
  private long current_round;


  public CassandraSpout(CassandraDao cassandraDao, int trainSize, int compareSize, int testSize, int batch_size) throws Exception {
    this.cassandraDao = cassandraDao;
    this.compareSize = compareSize;
    this.trainSize = trainSize;
    this.testSize = testSize;
    this.batch_size = batch_size;
    remaining_for_batch = 0;
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

    if(iterator == null || !iterator.hasNext())
    {
      if(roundlist.size()==0)
      {
        try {
//          getEventInfo.report();
          Thread.sleep(10000000);
        } catch (Exception e) {
          e.printStackTrace();
        }
        return;
      }
      long round = roundlist.remove(0);
      readRoundlist.add(round);
      System.out.println(new Date() + ": Round submission from cass spout =>" + round);

      if (readRoundlist.size() > compareSize) readRoundlist.remove(0);

      if(remaining_for_batch==0)
      {
        remaining_for_batch=batch_size;
        current_round=round;
      }
      remaining_for_batch--;

      ResultSet resultSet = getDataFromCassandra(round);
      iterator = resultSet.iterator();
    }
    Row row = iterator.next();
    String tweet = row.getString("tweet");
    String country = row.getString("country");
    Date tweetTime = row.getTimestamp("tweettime");
    long id = row.getLong("id");
    long retweetcount = row.getLong("retweetcount");
    long userid = row.getLong("userid");

//        System.out.println("cass spout: " + tweet);

    if(tweet == null || tweet.length() == 0) return;
    ArrayList<Long> tmp_roundlist = new ArrayList<>(readRoundlist);

//    System.out.println("Sending tweet from spout: " + tweet +" at round " + readRoundlist.get(readRoundlist.size()-1));
    // round | tweettime | id | country | retweetcount | tweet | userid
    if(iterator.hasNext()) {
      collector.emit(new Values(tweet, tmp_roundlist, false, current_round, "cassandra", country, tweetTime, id, retweetcount, userid));

    }
    else {
      collector.emit(new Values(tweet, tmp_roundlist, false, current_round, "cassandra", country, tweetTime, id, retweetcount, userid));
      collector.emit(new Values("BLOCKEND", tmp_roundlist, true, current_round, "cassandra", "USA", tweetTime, id, retweetcount, userid));
      collector.emit(new Values("BLOCKEND", tmp_roundlist, true, current_round, "cassandra", "CAN", tweetTime, id, retweetcount, userid));
      System.out.println(new Date() + ": Round end from cass spout =>" + current_round);
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void getRoundListFromCassandra(){
    ResultSet resultSet = null;
    try {
      resultSet = cassandraDao.getRounds();
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

      if(testSize!=Integer.MAX_VALUE) {
        while (roundlist.size() > testSize + trainSize)
          roundlist.remove(roundlist.size() - 1);
      }
      int i = 0;
      while(trainSize>i++)
        readRoundlist.add(roundlist.remove(0));

      while (roundlist.get(0) < 2034735)
        roundlist.remove(0);

      int j = roundlist.size()-1;

      while(roundlist.get(j)>2035083)
        roundlist.remove(j--);

//      for(int j=0; j < roundlist.size();j++)
//      {
//        if(roundlist.get(j)>2035083) roundlist.remove(j);
//      }



    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  public ResultSet getDataFromCassandra(long round) {
    ResultSet resultSet = null;
    try {
      resultSet = cassandraDao.getTweetsByRound(round);
    } catch (Exception e) {
      e.printStackTrace();
    }

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
    declarer.declare(new Fields("tweet", "dates","blockEnd", "round", "source", "country", "tweetTime", "id", "retweetcount", "userid"));
  }

}