package testtwitter.twitterWordCount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import org.apache.storm.shade.org.json.simple.JSONValue;
import org.apache.thrift7.TException;
import org.apache.thrift7.transport.TTransportException;
import spout.FileSpout;
import spout.TwitterSpout;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class WordCountTopology {

    private static LocalCluster localCluster;
    private static final String TWITTER_SPOUT_ID = "twitter-spout";
    private static final String FILE_SPOUT_ID = "file-spout";
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
    private static final String EVENT_COMPARE_BOLT = "event-compare-bolt";
    private static final String EVENT_DETECTOR_MANAGER_BOLT = "event-detector-manager-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    private static final String CONSUMER_KEY = "4w67IxCDr1zA8WGbgyTIHKMoU";
    private static final String CONSUMER_SECRET = "qa6P43R5Hq34Ge24dP8PwrXXNgPmEGWTf85wypx6F74neYASWH";
    private static final String ACCESS_TOKEN = "98180950-valFJ9XUpwrs0QUaco3nnQegx3ruQfABfhuOmeJvt";
    private static final String ACCESS_TOKEN_SECRET = "ZKtcJTCqsBWlQbsRDVWnENg9lEKQ8TKNR0tzy5pFVvssr";
    private static final int COUNT_THRESHOLD = 50;
    private static final double TIMEINTERVAL =  0.2;
    private static final int FILENUM = 28;
    private static final boolean TWEET_DOC_CREATION = false;


    public WordCountTopology( )
    {

    }

    public static void main(String[] args) throws Exception {

        Properties properties = loadProperties( "config.properties" );



        TwitterSpout spout = new TwitterSpout(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET, TIMEINTERVAL);
        FileSpout fileSpout = new FileSpout();

        PreprocessTweet preprocessor = new PreprocessTweet();
        SplitWordBolt splitBolt = new SplitWordBolt();
        SplitHashtagsBolt splitHashtagsBolt = new SplitHashtagsBolt();
        WordCountBolt countBolt = new WordCountBolt();
        WordCountBolt countHashtagBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt("sentences",30,COUNT_THRESHOLD,FILENUM);
        ReportBolt reportHashtagBolt = new ReportBolt("hashtags",60,COUNT_THRESHOLD,FILENUM);
        DocumentCreator documentCreator = new DocumentCreator(FILENUM, TWEET_DOC_CREATION);
        EventDetectorBolt eventDetectorBolt = new EventDetectorBolt(FILENUM);
        EventDetectorManagerBolt eventDetectorManagerBolt = new EventDetectorManagerBolt(COUNT_THRESHOLD);
        EventCompareBolt eventCompareBolt = new EventCompareBolt(FILENUM);

        System.out.println("time interval " + TIMEINTERVAL*60*60 + " & threshold " + COUNT_THRESHOLD);

        TopologyBuilder builder = new TopologyBuilder();

        createFolder(Integer.toString(FILENUM));
//        builder.setSpout(TWITTER_SPOUT_ID, spout);
//        builder.setBolt(PREPROCESS_SPOUT_ID, preprocessor).shuffleGrouping(TWITTER_SPOUT_ID);


        builder.setSpout(FILE_SPOUT_ID, fileSpout);
        builder.setBolt(PREPROCESS_SPOUT_ID, preprocessor).shuffleGrouping(FILE_SPOUT_ID);

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
        builder.setBolt( EVENT_COMPARE_BOLT, eventCompareBolt).globalGrouping(EVENT_DETECTOR_BOLT);


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

    protected static synchronized void loadTopologyPropertiesAndSubmit( Properties properties, Config config, StormTopology stormTopology )
            throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, InterruptedException, TTransportException {
        String stormExecutionMode = properties.getProperty( "storm.execution.mode" );
        String topologyName = properties.getProperty( "storm.topology.name" );
        String nimbusHost = properties.getProperty( "nimbus.host" );
        int nimbusPort = Integer.parseInt( properties.getProperty( "nimbus.port" ) );
        String stormZookeeperServers = properties.getProperty( "storm.zookeeper.serverss" );
        String stormPorts = properties.getProperty( "storm.ports" );

        config.setDebug( true );

        switch ( stormExecutionMode )
        {
            case ( "cluster" ):
                config.put( Config.NIMBUS_HOST, nimbusHost );
                config.put( Config.STORM_ZOOKEEPER_SERVERS, stormZookeeperServers );
                config.put( Config.SUPERVISOR_SLOTS_PORTS, stormPorts );
                config.setNumAckers( 3 );
                //config.setNumWorkers( 3 );
                Map storm_conf = Utils.readStormConfig();
                storm_conf.put("nimbus.host", nimbusHost);
                List<String> servers = splitString( stormZookeeperServers );
                storm_conf.put( "storm.zookeeper.servers", servers  );
                List<Integer> ports = splitInteger( stormPorts );
                storm_conf.put( "supervisor.slots.ports",ports  );
                String workingDir = System.getProperty("user.dir");
                String inputJar = workingDir + "/target/Demo-jar-with-dependencies.jar";
                NimbusClient nimbus = new NimbusClient(storm_conf, nimbusHost, nimbusPort );
                String uploadedJarLocation = StormSubmitter.submitJar(storm_conf,inputJar );
                try {
                    String jsonConf = JSONValue.toJSONString( storm_conf );
                    nimbus.getClient().submitTopology( topologyName,
                            uploadedJarLocation, jsonConf, stormTopology );
                } catch ( TException e ) {
                    e.printStackTrace();
                }
                Thread.sleep(6000);
                break;

            case ( "local" ):
                if(localCluster==null) localCluster = new LocalCluster();
                localCluster.submitTopology( topologyName, config, stormTopology );

        }
    }



    public static List<Integer> splitInteger( String sentence )
    {
        List<Integer> list = new ArrayList<>();
        String[] parts = sentence.split(",");

        for ( final String part : parts )
        {
            list.add( Integer.parseInt( part ) );
        }
        return list;
    }

    public static List<String> splitString( String sentence )
    {
        List<String> list = new ArrayList<>();
        String[] parts = sentence.split(",");

        for ( final String part : parts )
        {
            list.add( part );
        }
        return list;
    }

    protected static synchronized Config copyPropertiesToStormConfig( Properties properties )
    {
        Config stormConfig = new Config();
        for (String name : properties.stringPropertyNames()) {
            stormConfig.put(name, properties.getProperty(name));
        }
        return stormConfig;
    }

    protected static synchronized Properties loadProperties( String propertiesFile ) throws IOException
    {
        try {
            Properties properties = new Properties();
            InputStream inputStream = WordCountTopology.class.getClassLoader().getResourceAsStream( propertiesFile );
            properties.load( inputStream );
            inputStream.close();
            return properties;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
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