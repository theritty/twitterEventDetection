package testtwitter.twitterWordCount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cass.CassandraConnection;
import cass.CassandraDao;
import com.datastax.driver.core.Session;
import twitter4j.Status;
import twitter4j.User;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;


public class CassBolt extends BaseRichBolt
{
    private OutputCollector _collector;
    List<Object> values = new ArrayList<>();

    private Session session;
    private CassandraDao cassandraDao;

    public CassBolt(){

    }

    @Override
    public void prepare( final Map map, final TopologyContext topologyContext, final OutputCollector outputCollector )
    {
        try {
            _collector = outputCollector;
            CassandraConnection cassandraConnection = new CassandraConnection();
            session = cassandraConnection.connect();
            cassandraDao = new CassandraDao(session);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public void execute( final Tuple tuple )
    {

        Status tweet = (Status) tuple.getValueByField( "tweet" );
        String sentence = tweet.getText();
        User user = tweet.getUser();
        Date date = tweet.getCreatedAt();
        long id = tweet.getId();
        long retweetCount = tweet.getRetweetCount();

        if(sentence.startsWith("I'm at ")) return;
//        String sentence_preprocessed = sentence.replaceAll("[^A-Za-z0-9 _.,;:@^#+*=?&%£é\\{\\}\\(\\)\\[\\]<>|\\-$!\\\"'\\/$ığüşöçİÜĞÇÖŞ]*", "");

        values = new ArrayList<>();
        values.add(id);
        values.add(sentence);
        values.add(user.getId());
        values.add(date);
        values.add(retweetCount);



        try {
            cassandraDao.insert(session,values.toArray());
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
