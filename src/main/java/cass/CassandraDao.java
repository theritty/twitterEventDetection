package cass;


import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.List;

public class CassandraDao
{
    private PreparedStatement statement;
    private BoundStatement boundStatement;
    private BatchStatement batch = new BatchStatement();

    private static String CASS_TABLE_NAME = "tweets";
    private static String CASS_FIELDS = "(tweetid, tweet, userid, date, retweetcount)";
    private static String CASS_VALUES = "(?, ?, ?, ?, ?)";



    public CassandraDao(Session session) throws Exception {
        // Insert one record into the users table
        statement = session.prepare(
                "INSERT INTO " + CASS_TABLE_NAME + " " + CASS_FIELDS
                        + " VALUES " + CASS_VALUES + ";");

        boundStatement = new BoundStatement(statement);

    }
    public void insert( Session session, Object[] values ) throws Exception
    {
        session.executeAsync(boundStatement.bind(values));
//        if(batch.size() > 50) {
//            session.execute(batch);
//            batch.clear();
//        }
//        else
//        {
//            batch.add(boundStatement.bind(values));
//        }

    }


}

