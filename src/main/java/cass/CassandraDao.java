package cass;

import com.datastax.driver.core.*;

import java.io.Serializable;

public class CassandraDao implements Serializable
{
    private transient PreparedStatement statement;
    private transient BoundStatement boundStatement;

    private static String CASS_TABLE_NAME = "tweets3";
    private static String CASS_FIELDS = "(id, tweet, userid, tweettime, retweetcount, round, country)";
    private static String CASS_VALUES = "(?, ?, ?, ?, ?, ?, ?)";



    public CassandraDao() throws Exception {
        // Insert one record into the users table
        statement = CassandraConnection.connect().prepare(
                "INSERT INTO " + CASS_TABLE_NAME + " " + CASS_FIELDS
                        + " VALUES " + CASS_VALUES + ";");

        boundStatement = new BoundStatement(statement);

    }
    public void insert( Object[] values ) throws Exception
    {
        CassandraConnection.connect().executeAsync(boundStatement.bind(values));
    }

    public ResultSet readRules(String query) {
//        String query = "SELECT ruleDetails FROM rules";
        ResultSet result = CassandraConnection.connect().execute(query);

        return result;
    }


}

