package demo.cass;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;

import java.io.Serializable;

public class CassandraDao implements Serializable
{
    private transient PreparedStatement statement_tweets;
    private transient PreparedStatement statement_counts;
    private transient PreparedStatement statement_where;
    private transient BoundStatement boundStatement_tweets;
    private transient BoundStatement boundStatement_counts;
    private transient BoundStatement boundStatement_where;

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

//        boundStatement_tweets = new BoundStatement(statement_tweets);


        statement_counts = CassandraConnection.connect().prepare(
                "INSERT INTO " + COUNTS_TABLE_NAME + " " + COUNTS_FIELDS
                        + " VALUES " + COUNTS_VALUES + ";");

        statement_where = CassandraConnection.connect().prepare(
                "SELECT * FROM " + COUNTS_TABLE_NAME + " WHERE round=? AND word=? AND country=?;");

//        boundStatement_counts = new BoundStatement(statement_counts);

    }
    public void insertIntoTweets( Object[] values ) throws Exception
    {
        boundStatement_tweets = new BoundStatement(statement_tweets);
        ResultSetFuture rsf = CassandraConnection.connect().executeAsync(boundStatement_tweets.bind(values));
        checkError(rsf);
    }

    public void insertIntoCounts( Object[] values ) throws Exception
    {
        boundStatement_counts = new BoundStatement(statement_counts);
        ResultSetFuture rsf = CassandraConnection.connect().executeAsync(boundStatement_counts.bind(values));
        checkError(rsf);
    }

    public ResultSet getFromCounts( Object... values ) throws Exception
    {
        boundStatement_where = new BoundStatement(statement_where);
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_where.bind(values));

        return resultSet;
    }

    public ResultSet readRules(String query) {
        ResultSet result = CassandraConnection.connect().execute(query);

        return result;
    }

    static FutureCallback<ResultSet> callback =  new FutureCallback<ResultSet>() {
        @Override public void onSuccess(ResultSet result) {
        }

        @Override public void onFailure(Throwable t) {
            System.err.println("Error while reading Cassandra version: " + t.getMessage());
        }
    };

    public void checkError(ResultSetFuture future)
    {
        Futures.addCallback(future, callback, MoreExecutors.directExecutor());
    }

}

