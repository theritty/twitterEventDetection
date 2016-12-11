package topologyBuilder;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import eventDetector.bolts.*;
import cassandraConnector.CassandraDao;
import eventDetector.spout.CassandraSpout;
import tweetCollector.bolts.CassBolt;
import tweetCollector.bolts.CassCategoriesBolt;
import tweetCollector.bolts.PreprocessFromCassTweetBolt;
import tweetCollector.bolts.PreprocessTweetBolt;
import tweetCollector.spout.TwitterSpout;

import java.util.Properties;


public class BoltBuilder {
  public static StormTopology prepareBoltsForTwitter(Properties properties) {
    int COUNT_THRESHOLD = Integer.parseInt(properties.getProperty("topology.count.threshold"));
    double TIME_INTERVAL_IN_HOURS = Double.parseDouble(properties.getProperty("topology.time.interval"));
    String FILENUM = properties.getProperty("topology.file.number");
    String TWEETS_TABLE = properties.getProperty("tweets.table");
    String COUNTS_TABLE = properties.getProperty("counts.table");
    String EVENTS_TABLE = properties.getProperty("events.table");

    String CONSUMER_KEY = properties.getProperty("consumer.key");
    String CONSUMER_SECRET = properties.getProperty("consumer.secret");
    String ACCESS_TOKEN = properties.getProperty("access.token");
    String ACCESS_TOKEN_SECRET = properties.getProperty("access.token.secret");

    TopologyBuilder builder = new TopologyBuilder();

    TwitterSpout spout = new TwitterSpout(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN,
            ACCESS_TOKEN_SECRET, TIME_INTERVAL_IN_HOURS, Integer.parseInt(properties.getProperty("topology.train.size")),
            Integer.parseInt(properties.getProperty("topology.compare.size")));

    PreprocessTweetBolt preprocessor = new PreprocessTweetBolt();

    System.out.println("time interval " + TIME_INTERVAL_IN_HOURS * 60 * 60 + " & threshold " + COUNT_THRESHOLD);

    TopologyHelper.createFolder(Constants.STREAM_FILE_PATH + FILENUM);
    builder.setSpout(Constants.TWITTER_SPOUT_ID, spout);
    builder.setBolt(Constants.PREPROCESS_SPOUT_ID, preprocessor).shuffleGrouping(Constants.TWITTER_SPOUT_ID);


    builder.setBolt(Constants.CASS_BOLT_ID, new CassBolt(TIME_INTERVAL_IN_HOURS, TWEETS_TABLE, COUNTS_TABLE, EVENTS_TABLE)).
            shuffleGrouping(Constants.PREPROCESS_SPOUT_ID);
    return builder.createTopology();
  }

  public static StormTopology prepareBoltsForPreprocess(Properties properties) throws Exception {
    String TWEETS_TABLE = properties.getProperty("tweets.table");
    String COUNTS_TABLE = properties.getProperty("counts.table");
    String EVENTS_TABLE = properties.getProperty("events.table");

    CassandraDao cassandraDao = new CassandraDao(TWEETS_TABLE, COUNTS_TABLE, EVENTS_TABLE);
    System.out.println("Preparing Bolts...");
    TopologyBuilder builder = new TopologyBuilder();

    CassandraSpout cassandraSpout = new CassandraSpout(cassandraDao, 3,
            Integer.parseInt(properties.getProperty("topology.compare.size")),Integer.MAX_VALUE);

    PreprocessFromCassTweetBolt preprocessor = new PreprocessFromCassTweetBolt();
    TweetCategoryPredictionBolt tweetCategoryPredictionBolt = new TweetCategoryPredictionBolt();
    CassCategoriesBolt cassCategoriesBolt = new CassCategoriesBolt(cassandraDao);

    builder.setSpout(Constants.CASS_SPOUT_ID, cassandraSpout);

    builder.setBolt(Constants.PREPROCESS_SPOUT_ID, preprocessor).
            shuffleGrouping(Constants.CASS_SPOUT_ID);
    builder.setBolt(Constants.CLASSIFIER_BOLT_ID, tweetCategoryPredictionBolt).
            shuffleGrouping(Constants.PREPROCESS_SPOUT_ID);
    builder.setBolt(Constants.CASS_BOLT_ID, cassCategoriesBolt).
            shuffleGrouping(Constants.CLASSIFIER_BOLT_ID);

    return builder.createTopology();
  }

  public static StormTopology prepareBoltsForCassandraSpout(Properties properties) throws Exception {
    int COUNT_THRESHOLD = Integer.parseInt(properties.getProperty("topology.count.threshold"));
    String FILENUM = properties.getProperty("topology.file.number");
    double TFIDF_EVENT_RATE = Double.parseDouble(properties.getProperty("topology.tfidf.event.rate"));
    double RATE_FOR_SAME_EVENT = Double.parseDouble(properties.getProperty("topology.rate.for.same.event"));
    String TWEETS_TABLE = properties.getProperty("tweets.table");
    String COUNTS_TABLE = properties.getProperty("counts.table");
    String EVENTS_TABLE = properties.getProperty("events.table");


    System.out.println("Count threshold " + COUNT_THRESHOLD);
    TopologyHelper.createFolder(Constants.RESULT_FILE_PATH + FILENUM);
    TopologyHelper.createFolder(Constants.IMAGES_FILE_PATH + FILENUM);

    CassandraDao cassandraDao = new CassandraDao(TWEETS_TABLE, COUNTS_TABLE, EVENTS_TABLE);
    System.out.println("Preparing Bolts...");
    TopologyBuilder builder = new TopologyBuilder();

    CassandraSpout cassandraSpout = new CassandraSpout(cassandraDao, Integer.parseInt(properties.getProperty("topology.train.size")),
            Integer.parseInt(properties.getProperty("topology.compare.size")), Integer.MAX_VALUE);

    WordCountBolt countBoltUSA = new WordCountBolt(COUNT_THRESHOLD);
    WordCountBolt countBoltCAN = new WordCountBolt(COUNT_THRESHOLD);


    EventDetectorManagementBolt eventDetectorManagementBolt = new EventDetectorManagementBolt(Constants.RESULT_FILE_PATH, FILENUM);
    EventDetectorWithCassandraBolt eventDetectorBolt = new EventDetectorWithCassandraBolt(cassandraDao,
            Constants.RESULT_FILE_PATH, FILENUM, TFIDF_EVENT_RATE, TWEETS_TABLE);
    EventDetectorManagementBolt eventDetectorManagementBolt2 = new EventDetectorManagementBolt(Constants.RESULT_FILE_PATH, FILENUM);
    EventDetectorWithCassandraBolt eventDetectorBolt2 = new EventDetectorWithCassandraBolt(cassandraDao,
            Constants.RESULT_FILE_PATH, FILENUM, TFIDF_EVENT_RATE, TWEETS_TABLE);

//    EventDetectorManagerWithCassandraBolt eventDetectorManagerBolt = new EventDetectorManagerWithCassandraBolt(cassandraDao);
    EventCompareBolt eventCompareBolt = new EventCompareBolt(cassandraDao, FILENUM);

    builder.setSpout(Constants.CASS_SPOUT_ID, cassandraSpout,1);
    builder.setBolt(Constants.COUNTRY1_COUNT_BOLT_ID, countBoltUSA,5) .fieldsGrouping(Constants.CASS_SPOUT_ID, "USA", new Fields("word"));
    builder.setBolt(Constants.COUNTRY2_COUNT_BOLT_ID, countBoltCAN,5).fieldsGrouping(Constants.CASS_SPOUT_ID, "CAN", new Fields("word"));


    //USA
    builder.setBolt(Constants.COUNTRY1_REPORT_HASHTAG_BOLT_ID, eventDetectorManagementBolt).
            globalGrouping(Constants.COUNTRY1_COUNT_BOLT_ID);
    builder.setBolt( Constants.COUNTRY1_EVENT_DETECTOR_BOLT, eventDetectorBolt,2).
            fieldsGrouping(Constants.COUNTRY1_REPORT_HASHTAG_BOLT_ID, new Fields("key"));


    //CAN
    builder.setBolt(Constants.COUNTRY2_REPORT_HASHTAG_BOLT_ID, eventDetectorManagementBolt2).
            globalGrouping(Constants.COUNTRY2_COUNT_BOLT_ID);
    builder.setBolt( Constants.COUNTRY2_EVENT_DETECTOR_BOLT, eventDetectorBolt2,2).
            fieldsGrouping(Constants.COUNTRY2_REPORT_HASHTAG_BOLT_ID, new Fields("key"));


//    builder.setBolt( Constants.COUNTRY2_EVENT_COMPARE_BOLT, eventCompareBolt).
//            globalGrouping(Constants.COUNTRY1_EVENT_DETECTOR_BOLT).
//            globalGrouping(Constants.COUNTRY2_EVENT_DETECTOR_BOLT);

    return builder.createTopology();
  }
}

