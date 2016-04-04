package testtwitter.twitterWordCount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import spout.TwitterSpout;


public class WordCountTopology {

    private static final String TWITTER_SPOUT_ID = "twitter-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String SPLIT_HASHTAG_BOLT_ID = "split-hashtag-bolt";
    private static final String COUNT_BOLT_ID = "count-bolt";
    private static final String COUNT_HASHTAG_BOLT_ID = "count-hashtag-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String REPORT_HASHTAG_BOLT_ID = "report-hashtag-bolt";
    private static final String CASS_BOLT_ID = "cassandraBolt";
    private static final String DOCUMENT_CREATOR = "document-creator";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    private static final String CONSUMER_KEY = "IbHyNDuEUT6NptJ4MWPzjEzR7";
    private static final String CONSUMER_SECRET = "bqXPvV4JFFwX8rRm9PfMGO25ZKiYlgIALijov31hTYllmhNVSo";
    private static final String ACCESS_TOKEN = "98180950-valFJ9XUpwrs0QUaco3nnQegx3ruQfABfhuOmeJvt";
    private static final String ACCESS_TOKEN_SECRET = "ZKtcJTCqsBWlQbsRDVWnENg9lEKQ8TKNR0tzy5pFVvssr";


    public WordCountTopology( )
    {

    }

    public static void main(String[] args) throws Exception {
        TwitterSpout spout = new TwitterSpout(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET, 0.2);
        SplitWordBolt splitBolt = new SplitWordBolt();
        SplitHashtagsBolt splitHashtagsBolt = new SplitHashtagsBolt();
        WordCountBolt countBolt = new WordCountBolt();
        WordCountBolt countHashtagBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt("sentences",30);
        ReportBolt reportHashtagBolt = new ReportBolt("hashtags",60);
        DocumentCreator documentCreator = new DocumentCreator();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(TWITTER_SPOUT_ID, spout);

        builder.setBolt(SPLIT_BOLT_ID, splitBolt).shuffleGrouping(TWITTER_SPOUT_ID);
        builder.setBolt(COUNT_BOLT_ID, countBolt,5).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
        builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);

        builder.setBolt(SPLIT_HASHTAG_BOLT_ID, splitHashtagsBolt).shuffleGrouping(TWITTER_SPOUT_ID);
        builder.setBolt(COUNT_HASHTAG_BOLT_ID, countHashtagBolt,5).fieldsGrouping(SPLIT_HASHTAG_BOLT_ID, new Fields("word"));
        builder.setBolt(REPORT_HASHTAG_BOLT_ID, reportHashtagBolt).globalGrouping(COUNT_HASHTAG_BOLT_ID);

        builder.setBolt( CASS_BOLT_ID, new CassBolt() ).shuffleGrouping( TWITTER_SPOUT_ID);
        builder.setBolt( DOCUMENT_CREATOR, documentCreator).shuffleGrouping(TWITTER_SPOUT_ID);

        Config config = new Config();
//        config.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

        long sleep_minute=50;
        long sleep_seconds=0;
        Utils.sleep(sleep_minute*60*1000+sleep_seconds*1000);

        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }


}