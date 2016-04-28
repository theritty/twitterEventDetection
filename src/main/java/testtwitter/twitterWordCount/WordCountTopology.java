package testtwitter.twitterWordCount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import spout.TwitterSpout;

import java.io.File;


public class WordCountTopology {

    private static final String TWITTER_SPOUT_ID = "twitter-spout";
    private static final String PREPROCESS_SPOUT_ID = "preprocess-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String SPLIT_HASHTAG_BOLT_ID = "split-hashtag-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String COUNT_HASHTAG_BOLT_ID = "count-hashtag-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String REPORT_HASHTAG_BOLT_ID = "report-hashtag-bolt";
    private static final String CASS_BOLT_ID = "cassandraBolt";
    private static final String DOCUMENT_CREATOR = "document-creator";
    private static final String EVENT_DETECTOR_BOLT = "event-detector-bolt";
    private static final String EVENT_DETECTOR_BOLT2 = "event-detector-bolt2";
    private static final String EVENT_DETECTOR_BOLT3 = "event-detector-bolt3";
    private static final String EVENT_DETECTOR_MANAGER_BOLT = "event-detector-manager-bolt";
    private static final String EVENT_DETECTOR_MANAGER_BOLT2 = "event-detector-manager-bolt2";
    private static final String EVENT_DETECTOR_MANAGER_BOLT3 = "event-detector-manager-bolt3";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    private static final String CONSUMER_KEY = "4w67IxCDr1zA8WGbgyTIHKMoU";
    private static final String CONSUMER_SECRET = "qa6P43R5Hq34Ge24dP8PwrXXNgPmEGWTf85wypx6F74neYASWH";
    private static final String ACCESS_TOKEN = "98180950-valFJ9XUpwrs0QUaco3nnQegx3ruQfABfhuOmeJvt";
    private static final String ACCESS_TOKEN_SECRET = "ZKtcJTCqsBWlQbsRDVWnENg9lEKQ8TKNR0tzy5pFVvssr";
    private static final int COUNT_THRESHOLD = 50;
    private static final double TIMEINTERVAL =  0.2;
    private static final int FILENUM = 21;


    public WordCountTopology( )
    {

    }

    public static void main(String[] args) throws Exception {

        TwitterSpout spout = new TwitterSpout(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET, TIMEINTERVAL);
        PreprocessTweet preprocessor = new PreprocessTweet();
        SplitWordBolt splitBolt = new SplitWordBolt();
        SplitHashtagsBolt splitHashtagsBolt = new SplitHashtagsBolt();
        WordCountBolt countBolt = new WordCountBolt();
        WordCountBolt countHashtagBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt("sentences",30,COUNT_THRESHOLD,FILENUM);
        ReportBolt reportHashtagBolt = new ReportBolt("hashtags",60,COUNT_THRESHOLD,FILENUM);
        DocumentCreator documentCreator = new DocumentCreator(FILENUM);
        EventDetectorBolt eventDetectorBolt = new EventDetectorBolt(FILENUM);
        EventDetectorManagerBolt eventDetectorManagerBolt = new EventDetectorManagerBolt(COUNT_THRESHOLD);

        System.out.println("time interval " + TIMEINTERVAL*60*60 + " & threshold " + COUNT_THRESHOLD);

        TopologyBuilder builder = new TopologyBuilder();

        createFolder(Integer.toString(FILENUM));
        builder.setSpout(TWITTER_SPOUT_ID, spout);
        builder.setBolt(PREPROCESS_SPOUT_ID, preprocessor).shuffleGrouping(TWITTER_SPOUT_ID);

        builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(PREPROCESS_SPOUT_ID);
        builder.setBolt(COUNT_BOLT_ID, countBolt,5).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
        builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);

        builder.setBolt(SPLIT_HASHTAG_BOLT_ID, splitHashtagsBolt).shuffleGrouping(PREPROCESS_SPOUT_ID);
        builder.setBolt(COUNT_HASHTAG_BOLT_ID, countHashtagBolt,5).fieldsGrouping(SPLIT_HASHTAG_BOLT_ID, new Fields("word"));
        builder.setBolt(REPORT_HASHTAG_BOLT_ID, reportHashtagBolt).globalGrouping(COUNT_HASHTAG_BOLT_ID);

//        builder.setBolt( CASS_BOLT_ID, new CassBolt() ).shuffleGrouping( TWITTER_SPOUT_ID);
        builder.setBolt( DOCUMENT_CREATOR, documentCreator).shuffleGrouping(PREPROCESS_SPOUT_ID);
        builder.setBolt( EVENT_DETECTOR_MANAGER_BOLT, eventDetectorManagerBolt).globalGrouping(DOCUMENT_CREATOR).globalGrouping(COUNT_BOLT_ID).globalGrouping(COUNT_HASHTAG_BOLT_ID);
        builder.setBolt( EVENT_DETECTOR_BOLT, eventDetectorBolt,5).fieldsGrouping(EVENT_DETECTOR_MANAGER_BOLT, new Fields("key"));


        Config config = new Config();
//        config.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

        long sleep_hour=50;
        long sleep_minute=50;
        long sleep_seconds=0;
        Utils.sleep(sleep_hour*60*60*1000 + sleep_minute*60*1000 + sleep_seconds*1000);

        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }

    public static void createFolder(String fileName)
    {
        File theDir = new File(fileName);

        if (!theDir.exists()) {
            try{
                theDir.mkdir();
            }
            catch(SecurityException se){
            }
        }
    }

}