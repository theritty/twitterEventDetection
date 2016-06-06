package tweetCollector.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cassandraConnector.CassandraConnection;
import cassandraConnector.CassandraDao;
import com.datastax.driver.core.Session;
import twitter4j.GeoLocation;
import twitter4j.Status;
import twitter4j.User;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;


public class CassCategoriesBolt extends BaseRichBolt
{
  private OutputCollector _collector;
  List<Object> values = new ArrayList<>();

  private Session session;
  private CassandraDao cassandraDao;

  public CassCategoriesBolt( String tweets_table, String counts_table)
  {
    try {
      this.cassandraDao = new CassandraDao(tweets_table, counts_table);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void prepare( final Map map, final TopologyContext topologyContext, final OutputCollector outputCollector )
  {
    try {
      _collector = outputCollector;
      CassandraConnection cassandraConnection = new CassandraConnection();
      session = cassandraConnection.connect();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  @Override
  public void execute( final Tuple tuple )
  {
    //   "tweet", "round", "tweettime", "id", "retweetcount", "userid", "categories"));
    values = new ArrayList<>();
    values.add(tuple.getLongByField("id"));
    values.add(tuple.getStringByField("tweet"));
    values.add(tuple.getLongByField("userid"));
    values.add(tuple.getLongByField("tweetTime"));
    values.add(tuple.getLongByField("retweetcount"));
    values.add(tuple.getLongByField("round"));
    values.add(tuple.getStringByField("country"));
    if(tuple.getStringByField("categories").contains("politics"))
      values.add(true);
    else
      values.add(false);
    if(tuple.getStringByField("categories").contains("music"))
      values.add(true);
    else
      values.add(false);
    if(tuple.getStringByField("categories").contains("sports"))
      values.add(true);
    else
      values.add(false);

    try {
      cassandraDao.insertIntoTweets(values.toArray());
    } catch (Exception e) {
      e.printStackTrace();
    }

    try{
      _collector.emit(
              new Values(  ));

      _collector.ack( tuple );
    }catch (Exception e){
      System.out.println( "CassandraBolt Execute Error!" );
      e.printStackTrace();
    }
  }

  @Override
  public void declareOutputFields( final OutputFieldsDeclarer outputFieldsDeclarer )
  {
    outputFieldsDeclarer.declare( new Fields( ) );
  }
}
