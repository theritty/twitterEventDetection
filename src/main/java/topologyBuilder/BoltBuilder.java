package topologyBuilder;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import eventDetector.bolts.*;
import cassandraConnector.CassandraDao;
import eventDetector.spout.CassandraSpout;

import java.util.Properties;


public class BoltBuilder {

    public static StormTopology prepareBoltsForCassandraSpout(Properties properties) throws Exception {
        int COUNT_THRESHOLD = Integer.parseInt(properties.getProperty("topology.count.threshold"));
        String FILENUM = properties.getProperty("topology.file.number");
        double TFIDF_EVENT_RATE = Double.parseDouble(properties.getProperty("topology.tfidf.event.rate"));
        String TWEETS_TABLE = properties.getProperty("tweets.table");
        String COUNTS_TABLE = properties.getProperty("counts.table");
        String EVENTS_TABLE = properties.getProperty("events.table");


        System.out.println("Count threshold " + COUNT_THRESHOLD);
        TopologyHelper.createFolder(Constants.RESULT_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.IMAGES_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.TIMEBREAKDOWN_FILE_PATH + FILENUM);

        CassandraDao cassandraDao = new CassandraDao(TWEETS_TABLE, COUNTS_TABLE, EVENTS_TABLE);
        System.out.println("Preparing Bolts...");
        TopologyBuilder builder = new TopologyBuilder();
        CassandraSpout cassandraSpout = new CassandraSpout(cassandraDao, Integer.parseInt(properties.getProperty("topology.train.size")),
                Integer.parseInt(properties.getProperty("topology.compare.size")), 20, FILENUM);

        WordCountBolt countBoltUSA = new WordCountBolt(COUNT_THRESHOLD, FILENUM);
        WordCountBolt countBoltCAN = new WordCountBolt(COUNT_THRESHOLD, FILENUM);

        EventDetectorWithCassandraBolt eventDetectorBolt = new EventDetectorWithCassandraBolt(cassandraDao,
                Constants.RESULT_FILE_PATH, FILENUM, TFIDF_EVENT_RATE, TWEETS_TABLE);
        EventDetectorWithCassandraBolt eventDetectorBolt2 = new EventDetectorWithCassandraBolt(cassandraDao,
                Constants.RESULT_FILE_PATH, FILENUM, TFIDF_EVENT_RATE, TWEETS_TABLE);
        EventCompareBolt eventCompareBolt = new EventCompareBolt(cassandraDao, FILENUM);

        builder.setSpout(Constants.CASS_SPOUT_ID, cassandraSpout,1);
        builder.setBolt(Constants.COUNTRY1_COUNT_BOLT_ID, countBoltUSA,5) .fieldsGrouping(Constants.CASS_SPOUT_ID, "USA", new Fields("word"));
        builder.setBolt(Constants.COUNTRY2_COUNT_BOLT_ID, countBoltCAN,5).fieldsGrouping(Constants.CASS_SPOUT_ID, "CAN", new Fields("word"));

        //USA
        builder.setBolt( Constants.COUNTRY1_EVENT_DETECTOR_BOLT, eventDetectorBolt,2).
                shuffleGrouping(Constants.COUNTRY1_COUNT_BOLT_ID);
        //CAN
        builder.setBolt( Constants.COUNTRY2_EVENT_DETECTOR_BOLT, eventDetectorBolt2,2).
                shuffleGrouping(Constants.COUNTRY2_COUNT_BOLT_ID);

        builder.setBolt( Constants.COUNTRY2_EVENT_COMPARE_BOLT, eventCompareBolt).
            globalGrouping(Constants.COUNTRY1_EVENT_DETECTOR_BOLT).
            globalGrouping(Constants.COUNTRY2_EVENT_DETECTOR_BOLT);

        return builder.createTopology();
    }
}

