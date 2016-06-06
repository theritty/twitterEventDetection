package cassandraConnector;

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
    private transient PreparedStatement statement_tweet_get;
    private transient PreparedStatement statement_round_get;
    private transient BoundStatement boundStatement_tweets;
    private transient BoundStatement boundStatement_tweets_get;
    private transient BoundStatement boundStatement_rounds_get;
    private transient BoundStatement boundStatement_counts;
    private transient BoundStatement boundStatement_where;

    private static String TWEETS_FIELDS =   "(id, tweet, userid, tweettime, retweetcount, round, country, " +
                                            "class_politics, class_music,class_sports)";
    private static String TWEETS_VALUES = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static String TWEETS3_FIELDS = "(id, tweet, userid, tweettime, retweetcount, round, country)";
    private static String TWEETS3_VALUES = "(?, ?, ?, ?, ?, ?, ?)";

    private static String COUNTS_FIELDS = "(round, word, country, count, totalnumofwords)";
    private static String COUNTS_VALUES = "(?, ?, ?, ?, ?)";
    private String tweetsTable;
    private String countsTable;

    public CassandraDao(String tweetsTable, String countsTable) throws Exception {
        this.tweetsTable = tweetsTable;
        this.countsTable = countsTable;
        prepareAll();
    }

    private void prepareAll()
    {
        String tweetFields, tweetValues;

        if(tweetsTable.equals("tweets"))
        {
            tweetFields = TWEETS_FIELDS;
            tweetValues = TWEETS_VALUES;
        }
        else
        {
            tweetFields = TWEETS3_FIELDS;
            tweetValues = TWEETS3_VALUES;
        }

        if(statement_tweets==null) {
            statement_tweets = CassandraConnection.connect().prepare(
                    "INSERT INTO " + tweetsTable + " " + tweetFields
                            + " VALUES " + tweetValues + ";");
        }
        if(statement_counts==null) {
            statement_counts = CassandraConnection.connect().prepare(
                    "INSERT INTO " + countsTable + " " + COUNTS_FIELDS
                            + " VALUES " + COUNTS_VALUES + ";");
        }
        if(statement_where==null) {
            statement_where = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + countsTable + " WHERE round=? AND word=? AND country=?;");
        }
        if(statement_tweet_get==null) {
            statement_tweet_get = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + tweetsTable + " WHERE round=?;");
        }
        if(statement_round_get==null) {
            statement_round_get = CassandraConnection.connect().prepare(
                    "SELECT DISTINCT round FROM " + tweetsTable + ";");
        }
    }
    public void insertIntoTweets( Object[] values ) throws Exception
    {
        CassandraConnection.connect().executeAsync(boundStatement_tweets.bind(values));
    }

    public void insertIntoCounts( Object[] values ) throws Exception
    {
        prepareAll();
        boundStatement_counts = new BoundStatement(statement_counts);
        ResultSetFuture rsf = CassandraConnection.connect().executeAsync(boundStatement_counts.bind(values));
        checkError(rsf);
    }

    public ResultSet getFromCounts( Object... values ) throws Exception
    {
        prepareAll();
        boundStatement_where = new BoundStatement(statement_where);
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_where.bind(values));

        return resultSet;
    }

    public ResultSet getTweetsByRound( Object... values ) throws Exception
    {
        prepareAll();
        boundStatement_tweets_get = new BoundStatement(statement_tweet_get);
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_tweets_get.bind(values));

        return resultSet;
    }

    public ResultSet getRounds() throws Exception
    {
        prepareAll();
        boundStatement_rounds_get = new BoundStatement(statement_round_get);
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_rounds_get.bind());

        return resultSet;
    }

//    public ResultSet readRules(String query) {
//        prepareAll();
//        ResultSet result = CassandraConnection.connect().execute(query);
//
//        return result;
//    }

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

