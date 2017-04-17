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
    private transient PreparedStatement statement_events;
    private transient PreparedStatement statement_where;
    private transient PreparedStatement statement_tweet_get;
    private transient PreparedStatement statement_tweet_by_country_get;
    private transient PreparedStatement statement_round_get;
    private transient PreparedStatement statement_round_get_from_event;
    private transient PreparedStatement statement_events_get;
    private transient PreparedStatement statement_processtimes;
    private transient PreparedStatement statement_processtimes_get;
    private transient BoundStatement boundStatement_events;
    private transient BoundStatement boundStatement_events_get;
    private transient BoundStatement boundStatement_events_get_from_event;
    private transient BoundStatement boundStatement_tweets;
    private transient BoundStatement boundStatement_tweets_by_country_get;
    private transient BoundStatement boundStatement_tweets_get;
    private transient BoundStatement boundStatement_rounds_get;
    private transient BoundStatement boundStatement_counts;
    private transient BoundStatement boundStatement_where;
    private transient BoundStatement boundStatement_processtimes;
    private transient BoundStatement boundStatement_processtimes_get;

    private static String TWEETS_FIELDS =   "(id, tweet, userid, tweettime, retweetcount, round, country, " +
            "class_politics, class_music,class_sports)";
    private static String TWEETS_VALUES = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static String EVENTS_FIELDS = "(round, country, word, incrementpercent)";
    private static String EVENTS_VALUES = "(?, ?, ?, ?)";

    private static String COUNTS_FIELDS = "(round, word, country, count, totalnumofwords)";
    private static String COUNTS_VALUES = "(?, ?, ?, ?, ?)";

    private static String PROCESSTIMES_FIELDS =   "(row,column,id)";
    private static String PROCESSTIMES_VALUES = "(?, ?, ?)";

    private String tweetsTable;
    private String countsTable;
    private String eventsTable;
    private String processTimesTable;

    public CassandraDao(String tweetsTable, String countsTable, String eventsTable, String processTimesTable) throws Exception {
        this.tweetsTable = tweetsTable;
        this.countsTable = countsTable;
        this.eventsTable = eventsTable;
        this.processTimesTable = processTimesTable;

        prepareAll();
    }

    private void prepareAll()
    {
        if(statement_tweets==null) {
            statement_tweets = CassandraConnection.connect().prepare(
                    "INSERT INTO " + tweetsTable + " " + TWEETS_FIELDS
                            + " VALUES " + TWEETS_VALUES + ";");
        }
        if(statement_counts==null) {
            statement_counts = CassandraConnection.connect().prepare(
                    "INSERT INTO " + countsTable + " " + COUNTS_FIELDS
                            + " VALUES " + COUNTS_VALUES + ";");
        }
        if(statement_events==null) {
            statement_events = CassandraConnection.connect().prepare(
                    "INSERT INTO " + eventsTable + " " + EVENTS_FIELDS
                            + " VALUES " + EVENTS_VALUES + ";");
        }
        if(statement_where==null) {
            statement_where = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + countsTable + " WHERE round=? AND word=? AND country=?;");
        }
        if(statement_tweet_get==null) {
            statement_tweet_get = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + tweetsTable + " WHERE round=?;");
        }
        if(statement_tweet_by_country_get==null) {
            statement_tweet_by_country_get = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + tweetsTable + " WHERE round=? AND country=?;");
        }
        if(statement_round_get==null) {
            statement_round_get = CassandraConnection.connect().prepare(
                    "SELECT DISTINCT round FROM " + tweetsTable + ";");
        }
        if(statement_round_get_from_event==null) {
            statement_round_get_from_event = CassandraConnection.connect().prepare(
                    "SELECT DISTINCT round FROM " + eventsTable + ";");
        }
        if(statement_events_get==null) {
            statement_events_get = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + eventsTable + " WHERE round=? AND country=?;");
        }
        if(statement_processtimes==null) {
            statement_processtimes = CassandraConnection.connect().prepare(
                    "INSERT INTO " + processTimesTable + " " + PROCESSTIMES_FIELDS
                            + " VALUES " + PROCESSTIMES_VALUES + ";");
        }
        if(statement_processtimes_get==null) {
            statement_processtimes_get = CassandraConnection.connect().prepare(
                    "SELECT * FROM " + processTimesTable );
        }

        if(boundStatement_tweets == null)
            boundStatement_tweets = new BoundStatement(statement_tweets);
        if(boundStatement_events == null)
            boundStatement_events = new BoundStatement(statement_events);
        if(boundStatement_counts == null)
            boundStatement_counts = new BoundStatement(statement_counts);
        if(boundStatement_where == null)
            boundStatement_where = new BoundStatement(statement_where);
        if(boundStatement_events_get == null)
            boundStatement_events_get = new BoundStatement(statement_events_get);
        if(boundStatement_tweets_get == null)
            boundStatement_tweets_get = new BoundStatement(statement_tweet_get);
        if(boundStatement_tweets_by_country_get == null)
            boundStatement_tweets_by_country_get = new BoundStatement(statement_tweet_by_country_get);
        if(boundStatement_rounds_get == null)
            boundStatement_rounds_get = new BoundStatement(statement_round_get);
        if(boundStatement_events_get_from_event == null)
            boundStatement_events_get_from_event = new BoundStatement(statement_round_get_from_event);
        if(boundStatement_processtimes_get == null)
            boundStatement_processtimes_get = new BoundStatement(statement_processtimes_get);
        if(boundStatement_processtimes == null)
            boundStatement_processtimes = new BoundStatement(statement_processtimes);
    }
    public void insertIntoTweets( Object[] values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().executeAsync(boundStatement_tweets.bind(values));
    }

    public void insertIntoEvents( Object... values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().executeAsync(boundStatement_events.bind(values));
    }

    public void insertIntoCounts( Object[] values ) throws Exception
    {
        prepareAll();
        ResultSetFuture rsf = CassandraConnection.connect().executeAsync(boundStatement_counts.bind(values));
        checkError(rsf);
    }

    public ResultSet getFromCounts( Object... values ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_where.bind(values));

        return resultSet;
    }

    public ResultSet getFromEvents( Object... values ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_events_get.bind(values));

        return resultSet;
    }

    public ResultSet getTweetsByRound( Object... values ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_tweets_get.bind(values));

        return resultSet;
    }

    public ResultSet getTweetsByRoundAndCountry( Object... values ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_tweets_by_country_get.bind(values));
        return resultSet;
    }

    public ResultSet getRounds() throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_rounds_get.bind());

        return resultSet;
    }

    public ResultSet getRoundsFromEvents() throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_events_get_from_event.bind());

        return resultSet;
    }

    public void insertIntoProcessTimes( Object[] values ) throws Exception
    {
        prepareAll();
        CassandraConnection.connect().executeAsync(boundStatement_processtimes.bind(values));
    }

    public ResultSet getProcessTimes(  ) throws Exception
    {
        prepareAll();
        ResultSet resultSet = CassandraConnection.connect().execute(boundStatement_processtimes_get.bind());

        return resultSet;
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

