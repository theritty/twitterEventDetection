package demo.cass;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;

import java.io.Serializable;

public class CassandraDao implements Serializable
{
    private transient PreparedStatement statement_tweets;
    private transient PreparedStatement statement_counts;
    private transient BoundStatement boundStatement_tweets;
    private transient BoundStatement boundStatement_counts;

    private static String TWEETS_TABLE_NAME = "tweets3";
    private static String TWEETS_FIELDS = "(id, tweet, userid, tweettime, retweetcount, round, country)";
    private static String TWEETS_VALUES = "(?, ?, ?, ?, ?, ?, ?)";

    private static String COUNTS_TABLE_NAME = "counts";
    private static String COUNTS_FIELDS = "(round, word, country, count, totalnumofwords)";
    private static String COUNTS_VALUES = "(?, ?, ?, ?, ?)";


    public CassandraDao() throws Exception {
        // Insert one record into the users table
        statement_tweets = CassandraConnection.connect().prepare(
                "INSERT INTO " + TWEETS_TABLE_NAME + " " + TWEETS_FIELDS
                        + " VALUES " + TWEETS_VALUES + ";");

        boundStatement_tweets = new BoundStatement(statement_tweets);


        statement_counts = CassandraConnection.connect().prepare(
                "INSERT INTO " + COUNTS_TABLE_NAME + " " + COUNTS_FIELDS
                        + " VALUES " + COUNTS_VALUES + ";");

        boundStatement_counts = new BoundStatement(statement_counts);

    }
    public void insertIntoTweets( Object[] values ) throws Exception
    {
        CassandraConnection.connect().executeAsync(boundStatement_tweets.bind(values));
    }

    public void insertIntoCounts( Object[] values ) throws Exception
    {
        CassandraConnection.connect().executeAsync(boundStatement_counts.bind(values));
    }

    public ResultSet readRules(String query) {
        ResultSet result = CassandraConnection.connect().execute(query);

        return result;
    }


}

