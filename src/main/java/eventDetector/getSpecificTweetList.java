package eventDetector;

import cassandraConnector.CassandraDao;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by ceren on 12.06.2016.
 */
public class getSpecificTweetList {
    public static void main(String[] args) throws Exception {
        getSpecificTweets(2034243,"katy");
    }

    public static void getSpecificTweets(long round, String word) throws Exception {
        CassandraDao cassandraDao = new CassandraDao("tweets", "counts", "events");
        ResultSet resultSet = cassandraDao.getTweetsByRound(round);

        Iterator<Row> iterator = resultSet.iterator();
        while(iterator.hasNext())
        {
            Row row = iterator.next();
            if(row.getString("tweet").contains(word))
                System.out.println(row.getString("tweet"));
        }
    }
    public static void prepareDemo() throws Exception {
        CassandraDao cassandraDao = new CassandraDao("tweets", "counts", "events");
        CassandraDao cassandraDao2 = new CassandraDao("tweetsdemo", "countsdemo", "eventsdemo");
        long round = 2034735;
        while(round<=2034751) {
            prepare(cassandraDao,cassandraDao2,round);
            round+=2;
            System.out.println(round + " done");
        }
    }
    public static void prepare(CassandraDao cassandraDao,CassandraDao cassandraDao2, long round) throws Exception {
        ResultSet resultSet = cassandraDao.getTweetsByRound(round);

        Iterator<Row> iterator = resultSet.iterator();
        while(iterator.hasNext())
        {
            Row row = iterator.next();

            List<Object> values = new ArrayList<>();
            values.add(row.getLong("id"));
            values.add(row.getString("tweet"));
            values.add(row.getLong("userid"));
            values.add(row.getTimestamp("tweettime"));
            values.add(row.getLong("retweetcount"));
            values.add(row.getLong("round"));
            values.add(row.getString("country"));
            values.add(row.getBool("class_politics"));
            values.add(row.getBool("class_music"));
            values.add(row.getBool("class_sports"));

            cassandraDao2.insertIntoTweets(values.toArray());
        }
    }
}
