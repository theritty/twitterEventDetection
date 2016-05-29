package demo.topologies.topologyBuild;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import demo.bolts.*;
import demo.bolts.EventDetectorManagerWithCassandraBolt;
import demo.bolts.EventDetectorWithCassandraBolt;
import demo.cass.CassandraDao;
import demo.spout.CassandraSpout;

import java.util.Properties;


public class BoltBuilder {
    public static StormTopology prepareBoltsForCassandraSpout(Properties properties) throws Exception {
    int COUNT_THRESHOLD = Integer.parseInt(properties.getProperty("topology.count.threshold"));
    int FILENUM = Integer.parseInt(properties.getProperty("topology.file.number"));
    double TFIDF_EVENT_RATE = Double.parseDouble(properties.getProperty("topology.tfidf.event.rate"));
    double RATE_FOR_SAME_EVENT = Double.parseDouble(properties.getProperty("topology.rate.for.same.event"));

      System.out.println("Preparing Bolts...");
    TopologyBuilder builder = new TopologyBuilder();

    CassandraSpout cassandraSpout = new CassandraSpout(Integer.parseInt(properties.getProperty("topology.train.size")),
            Integer.parseInt(properties.getProperty("topology.compare.size")));

//        PreprocessTweetBolt preprocessor = new PreprocessTweetBolt();
    SplitWordBolt splitBolt1 = new SplitWordBolt("USA");
    SplitWordBolt splitBolt2 = new SplitWordBolt("CAN");
    SplitHashtagsBolt splitHashtagsBolt1 = new SplitHashtagsBolt("USA");
    SplitHashtagsBolt splitHashtagsBolt2 = new SplitHashtagsBolt("CAN");
    WordCountBolt countBolt = new WordCountBolt(COUNT_THRESHOLD);
    WordCountBolt countHashtagBolt = new WordCountBolt(COUNT_THRESHOLD);
    ReportBolt reportBolt = new ReportBolt("sentences", COUNT_THRESHOLD, Constants.RESULT_FILE_PATH, FILENUM);
    ReportBolt reportHashtagBolt = new ReportBolt("hashtags", COUNT_THRESHOLD, Constants.RESULT_FILE_PATH, FILENUM);
//        DocumentCreator documentCreator = new DocumentCreator(Constants.RESULT_FILE_PATH, FILENUM, TWEET_DOC_CREATION);
    EventDetectorWithCassandraBolt eventDetectorBolt = new EventDetectorWithCassandraBolt(Constants.RESULT_FILE_PATH, FILENUM, TFIDF_EVENT_RATE,
            Integer.parseInt(properties.getProperty("topology.input.file.number")) );
    EventDetectorManagerWithCassandraBolt eventDetectorManagerBolt = new EventDetectorManagerWithCassandraBolt();
    EventCompareBolt eventCompareBolt = new EventCompareBolt(Constants.RESULT_FILE_PATH, FILENUM, RATE_FOR_SAME_EVENT);

    System.out.println("Count threshold " + COUNT_THRESHOLD);

    TopologyHelper.createFolder(Constants.RESULT_FILE_PATH + Integer.toString(FILENUM));
    TopologyHelper.createFolder(Constants.IMAGES_FILE_PATH + Integer.toString(FILENUM));

    builder.setSpout(Constants.FILE_SPOUT_ID, cassandraSpout);


    builder.setBolt(Constants.COUNTRY1_SPLIT_BOLT_ID, splitBolt1).
            shuffleGrouping(Constants.FILE_SPOUT_ID);
    builder.setBolt(Constants.COUNTRY1_COUNT_BOLT_ID, countBolt,5).
            fieldsGrouping(Constants.COUNTRY1_SPLIT_BOLT_ID, new Fields("word"));
    builder.setBolt(Constants.COUNTRY1_REPORT_BOLT_ID, reportBolt).
            globalGrouping(Constants.COUNTRY1_COUNT_BOLT_ID);

    builder.setBolt(Constants.COUNTRY1_SPLIT_HASHTAG_BOLT_ID, splitHashtagsBolt1).
            shuffleGrouping(Constants.FILE_SPOUT_ID);
    builder.setBolt(Constants.COUNTRY1_COUNT_HASHTAG_BOLT_ID, countHashtagBolt,5).
            fieldsGrouping(Constants.COUNTRY1_SPLIT_HASHTAG_BOLT_ID, new Fields("word"));
    builder.setBolt(Constants.COUNTRY1_REPORT_HASHTAG_BOLT_ID, reportHashtagBolt).
            globalGrouping(Constants.COUNTRY1_COUNT_HASHTAG_BOLT_ID);

//    builder.setBolt( Constants.COUNTRY1_EVENT_DETECTOR_MANAGER_BOLT, eventDetectorManagerBolt).
//            globalGrouping(Constants.COUNTRY1_COUNT_BOLT_ID).
//            globalGrouping(Constants.COUNTRY1_COUNT_HASHTAG_BOLT_ID);
//    builder.setBolt( Constants.COUNTRY1_EVENT_DETECTOR_BOLT, eventDetectorBolt,5).
//            fieldsGrouping(Constants.COUNTRY1_EVENT_DETECTOR_MANAGER_BOLT, new Fields("key"));
//    builder.setBolt( Constants.COUNTRY1_EVENT_COMPARE_BOLT, eventCompareBolt).
//            globalGrouping(Constants.COUNTRY1_EVENT_DETECTOR_BOLT);



    builder.setBolt(Constants.COUNTRY2_SPLIT_BOLT_ID, splitBolt2).
            shuffleGrouping(Constants.FILE_SPOUT_ID);
    builder.setBolt(Constants.COUNTRY2_COUNT_BOLT_ID, countBolt,5).
            fieldsGrouping(Constants.COUNTRY2_SPLIT_BOLT_ID, new Fields("word"));
    builder.setBolt(Constants.COUNTRY2_REPORT_BOLT_ID, reportBolt).
            globalGrouping(Constants.COUNTRY2_COUNT_BOLT_ID);

    builder.setBolt(Constants.COUNTRY2_SPLIT_HASHTAG_BOLT_ID, splitHashtagsBolt2).
            shuffleGrouping(Constants.FILE_SPOUT_ID);
    builder.setBolt(Constants.COUNTRY2_COUNT_HASHTAG_BOLT_ID, countHashtagBolt,5).
            fieldsGrouping(Constants.COUNTRY2_SPLIT_HASHTAG_BOLT_ID, new Fields("word"));
    builder.setBolt(Constants.COUNTRY2_REPORT_HASHTAG_BOLT_ID, reportHashtagBolt).
            globalGrouping(Constants.COUNTRY2_COUNT_HASHTAG_BOLT_ID);

    builder.setBolt( Constants.COUNTRY2_EVENT_DETECTOR_MANAGER_BOLT, eventDetectorManagerBolt).
            globalGrouping(Constants.COUNTRY2_COUNT_BOLT_ID).
            globalGrouping(Constants.COUNTRY2_COUNT_HASHTAG_BOLT_ID).
            globalGrouping(Constants.COUNTRY1_COUNT_BOLT_ID).
            globalGrouping(Constants.COUNTRY1_COUNT_HASHTAG_BOLT_ID);
    builder.setBolt( Constants.COUNTRY2_EVENT_DETECTOR_BOLT, eventDetectorBolt,8).
            fieldsGrouping(Constants.COUNTRY2_EVENT_DETECTOR_MANAGER_BOLT, new Fields("key"));
    builder.setBolt( Constants.COUNTRY2_EVENT_COMPARE_BOLT, eventCompareBolt).
            globalGrouping(Constants.COUNTRY2_EVENT_DETECTOR_BOLT);

    return builder.createTopology();
  }
}
