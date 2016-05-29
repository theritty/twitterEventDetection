package trials.cass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;


public class CassandraConnection {

    private static String CASS_CONTACT_POINT = "127.0.0.1";
    private static String CASS_KEYSPACE = "tweetcollection";
    static Session session = null;

    public static Session connect(  )
    {
        if(session != null) return session;
        Cluster cluster;
        cluster = Cluster.builder()
                .addContactPoint( CASS_CONTACT_POINT )
                .withRetryPolicy( DefaultRetryPolicy.INSTANCE)
                         .withLoadBalancingPolicy(
                                 new TokenAwarePolicy(
                                         new DCAwareRoundRobinPolicy())
                         ).build();
        session = cluster.connect( CASS_KEYSPACE );

        return session;
    }
}
