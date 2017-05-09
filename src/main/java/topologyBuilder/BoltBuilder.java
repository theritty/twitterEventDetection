package topologyBuilder;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
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
        String PROCESSED_TABLE = properties.getProperty("processed.table");

        int CAN_TASK_NUM= Integer.parseInt(properties.getProperty("can.taskNum"));
        int USA_TASK_NUM= Integer.parseInt(properties.getProperty("usa.taskNum"));
        int NUM_WORKERS= Integer.parseInt(properties.getProperty("num.workers"));
        int NUM_DETECTORS= Integer.parseInt(properties.getProperty("num.detectors.per.country"));
        int NUM_COUNTRIES= Integer.parseInt(properties.getProperty("num.countries"));


        System.out.println("Count threshold " + COUNT_THRESHOLD);
        TopologyHelper.createFolder(Constants.RESULT_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.IMAGES_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.TIMEBREAKDOWN_FILE_PATH + FILENUM);

        System.out.println("Preparing Bolts...");
        TopologyBuilder builder = new TopologyBuilder();

        CassandraDao cassandraDao = new CassandraDao(TWEETS_TABLE, COUNTS_TABLE, EVENTS_TABLE, PROCESSED_TABLE);
        CassandraSpout cassandraSpout = new CassandraSpout(cassandraDao, FILENUM, USA_TASK_NUM, CAN_TASK_NUM, NUM_WORKERS, NUM_DETECTORS*NUM_COUNTRIES);
        WordCountBolt countBoltUSA = new WordCountBolt(COUNT_THRESHOLD, FILENUM, "USA", cassandraDao, NUM_DETECTORS, NUM_WORKERS+CAN_TASK_NUM+USA_TASK_NUM+3);
        WordCountBolt countBoltCAN = new WordCountBolt(COUNT_THRESHOLD, FILENUM, "CAN", cassandraDao, NUM_DETECTORS, NUM_WORKERS+CAN_TASK_NUM+USA_TASK_NUM+3+NUM_DETECTORS);
        EventDetectorWithCassandraBolt eventDetectorBoltUSA = new EventDetectorWithCassandraBolt(cassandraDao,
                Constants.RESULT_FILE_PATH, FILENUM, TFIDF_EVENT_RATE, Integer.parseInt(properties.getProperty("topology.compare.size")), "USA");
        EventDetectorWithCassandraBolt eventDetectorBoltCAN = new EventDetectorWithCassandraBolt(cassandraDao,
                Constants.RESULT_FILE_PATH, FILENUM, TFIDF_EVENT_RATE, Integer.parseInt(properties.getProperty("topology.compare.size")), "CAN");

        EventCompareBolt eventCompareBolt = new EventCompareBolt(cassandraDao, FILENUM);
        builder.setSpout(Constants.CASS_SPOUT_ID, cassandraSpout,1);

        builder.setBolt(Constants.COUNTRY1_COUNT_BOLT_ID, countBoltUSA,USA_TASK_NUM).directGrouping(Constants.CASS_SPOUT_ID);
        builder.setBolt(Constants.COUNTRY2_COUNT_BOLT_ID, countBoltCAN,CAN_TASK_NUM).directGrouping(Constants.CASS_SPOUT_ID);

        builder.setBolt( Constants.COUNTRY1_EVENT_DETECTOR_BOLT, eventDetectorBoltUSA,NUM_DETECTORS).directGrouping(Constants.COUNTRY1_COUNT_BOLT_ID);
        builder.setBolt( Constants.COUNTRY2_EVENT_DETECTOR_BOLT, eventDetectorBoltCAN,NUM_DETECTORS).directGrouping(Constants.COUNTRY2_COUNT_BOLT_ID);

        builder.setBolt( Constants.EVENT_COMPARE_BOLT, eventCompareBolt,1).
                globalGrouping(Constants.COUNTRY1_EVENT_DETECTOR_BOLT).
                globalGrouping(Constants.COUNTRY2_EVENT_DETECTOR_BOLT);

        return builder.createTopology();
    }
}

