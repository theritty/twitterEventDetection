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
        double RATE_FOR_SAME_EVENT = Double.parseDouble(properties.getProperty("topology.rate.for.same.event"));
        String TWEETS_TABLE = properties.getProperty("tweets.table");
        String COUNTS_TABLE = properties.getProperty("counts.table");
        String EVENTS_TABLE = properties.getProperty("events.table");
        String PROCESSEDTWEET_TABLE = properties.getProperty("processed_tweets.table");

        long START_ROUND = Long.parseLong(properties.getProperty("start.round"));

        int CAN_TASK_NUM= Integer.parseInt(properties.getProperty("can.taskNum"));
        int USA_TASK_NUM= Integer.parseInt(properties.getProperty("usa.taskNum"));
        long END_ROUND = Long.parseLong(properties.getProperty("end.round"));


        System.out.println("Count threshold " + COUNT_THRESHOLD);
        TopologyHelper.createFolder(Constants.RESULT_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.IMAGES_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.TIMEBREAKDOWN_FILE_PATH + FILENUM);

        CassandraDao cassandraDao = new CassandraDao(TWEETS_TABLE, COUNTS_TABLE, EVENTS_TABLE, PROCESSEDTWEET_TABLE);
        System.out.println("Preparing Bolts...");
        TopologyBuilder builder = new TopologyBuilder();

//        CassandraSpout cassandraSpout = new CassandraSpout(cassandraDao, Integer.parseInt(properties.getProperty("topology.train.size")),
//                Integer.parseInt(properties.getProperty("topology.compare.size")), Integer.MAX_VALUE, FILENUM);
        CassandraSpout cassandraSpout = new CassandraSpout(cassandraDao, Integer.parseInt(properties.getProperty("topology.train.size")),
                Integer.parseInt(properties.getProperty("topology.compare.size")), 20, FILENUM, START_ROUND, END_ROUND, CAN_TASK_NUM, USA_TASK_NUM);

        WordCountBolt countBoltUSA = new WordCountBolt(COUNT_THRESHOLD, FILENUM, "USA", cassandraDao);
        WordCountBolt countBoltCAN = new WordCountBolt(COUNT_THRESHOLD, FILENUM, "CAN", cassandraDao);


//        EventDetectorManagementBolt eventDetectorManagementBolt = new EventDetectorManagementBolt(Constants.RESULT_FILE_PATH, FILENUM);
        EventDetectorWithCassandraBolt eventDetectorBolt = new EventDetectorWithCassandraBolt(cassandraDao,
                Constants.RESULT_FILE_PATH, FILENUM, TFIDF_EVENT_RATE, TWEETS_TABLE, "USA", USA_TASK_NUM, CAN_TASK_NUM);
//        EventDetectorManagementBolt eventDetectorManagementBolt2 = new EventDetectorManagementBolt(Constants.RESULT_FILE_PATH, FILENUM);
        EventDetectorWithCassandraBolt eventDetectorBolt2 = new EventDetectorWithCassandraBolt(cassandraDao,
                Constants.RESULT_FILE_PATH, FILENUM, TFIDF_EVENT_RATE, TWEETS_TABLE, "CAN", USA_TASK_NUM, CAN_TASK_NUM);

//    EventDetectorManagerWithCassandraBolt eventDetectorManagerBolt = new EventDetectorManagerWithCassandraBolt(cassandraDao);
        EventCompareBolt eventCompareBolt = new EventCompareBolt(cassandraDao, FILENUM);






        builder.setSpout(Constants.CASS_SPOUT_ID, cassandraSpout,1);
        builder.setBolt(Constants.COUNTRY1_COUNT_BOLT_ID, countBoltUSA,USA_TASK_NUM).directGrouping(Constants.CASS_SPOUT_ID);
        builder.setBolt(Constants.COUNTRY2_COUNT_BOLT_ID, countBoltCAN,CAN_TASK_NUM).directGrouping(Constants.CASS_SPOUT_ID);

        //USA
        builder.setBolt( Constants.COUNTRY1_EVENT_DETECTOR_BOLT, eventDetectorBolt,2).
                directGrouping(Constants.COUNTRY1_COUNT_BOLT_ID);


        //CAN
        builder.setBolt( Constants.COUNTRY2_EVENT_DETECTOR_BOLT, eventDetectorBolt2,2).
                directGrouping(Constants.COUNTRY2_COUNT_BOLT_ID);


//        //USA
//        builder.setBolt(Constants.COUNTRY1_REPORT_HASHTAG_BOLT_ID, eventDetectorManagementBolt).
//                globalGrouping(Constants.COUNTRY1_COUNT_BOLT_ID);
//        builder.setBolt( Constants.COUNTRY1_EVENT_DETECTOR_BOLT, eventDetectorBolt,2).
//                fieldsGrouping(Constants.COUNTRY1_REPORT_HASHTAG_BOLT_ID, new Fields("key"));
//
//
//        //CAN
//        builder.setBolt(Constants.COUNTRY2_REPORT_HASHTAG_BOLT_ID, eventDetectorManagementBolt2).
//                globalGrouping(Constants.COUNTRY2_COUNT_BOLT_ID);
//        builder.setBolt( Constants.COUNTRY2_EVENT_DETECTOR_BOLT, eventDetectorBolt2,2).
//                fieldsGrouping(Constants.COUNTRY2_REPORT_HASHTAG_BOLT_ID, new Fields("key"));


    builder.setBolt( Constants.COUNTRY2_EVENT_COMPARE_BOLT, eventCompareBolt,1).
            globalGrouping(Constants.COUNTRY1_EVENT_DETECTOR_BOLT).
            globalGrouping(Constants.COUNTRY2_EVENT_DETECTOR_BOLT);

        return builder.createTopology();
    }
}

