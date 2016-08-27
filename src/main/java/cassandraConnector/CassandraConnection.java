package cassandraConnector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;


public class CassandraConnection {

    private static String CASS_CONTACT_POINT = "demo-cassandra1";
    private static String CASS_KEYSPACE = "tweetcollection";
    static Session session = null;

    public static Session connect(  )
    {
        if(session != null) return session;
        Cluster cluster;

        cluster = Cluster.builder()
                .addContactPoint(CASS_CONTACT_POINT)
                .build();
        session = cluster.connect( CASS_KEYSPACE );

        return session;
    }
}
