package eventDetector.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import eventDetector.algorithms.TFIDFCalculatorWithCassandra;
import cassandraConnector.CassandraDao;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

public class EventDetectorWithCassandraBolt extends BaseRichBolt {

    private OutputCollector collector;
    private String filePath;
    private double tfidfEventRate;
    private CassandraDao cassandraDao;
    private String tweetTable;
    private String componentId;
    private long currentRound = 0;
    private String fileNum;

    public EventDetectorWithCassandraBolt(CassandraDao cassandraDao, String filePath, String fileNum, double tfidfEventRate, String tweetTable )
    {
        this.tfidfEventRate = tfidfEventRate;
        this.filePath = filePath + fileNum;
        this.cassandraDao = cassandraDao;
        this.tweetTable = tweetTable;
        this.fileNum = fileNum + "/";
    }

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.componentId = String.valueOf(UUID.randomUUID());
    }

    @Override
    public void execute(Tuple tuple) {

        ArrayList<Long> rounds = (ArrayList<Long>)tuple.getValueByField("rounds");
        String key = tuple.getStringByField("key");
        String country = (String) tuple.getValueByField( "country" );
        long round = tuple.getLongByField("round");


        ArrayList<Double> tfidfs = new ArrayList<>();
        if(currentRound < round) {
            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + round + ".txt",
                    "Detector bolt " + componentId + " start of round " + round + " at " + new Date());
            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
                    "Detector bolt " + componentId + " end of round " + currentRound + " at " + new Date());
            currentRound = round;
        }

        if(currentRound > round) {
            System.out.println("ignoooooooooooooooooreeeeeeeeeeeeeee");
        }

        for (long roundNum: rounds)
        {
            TFIDFCalculatorWithCassandra calculator = new TFIDFCalculatorWithCassandra();
            tfidfs.add(calculator.tfIdf(cassandraDao, rounds,key,roundNum,country, tweetTable));
        }
        boolean allzero=true;
        for(double tfidf: tfidfs)
        {
            if(tfidf != 0.0)
            {
                allzero=false;
                break;
            }
        }

        if(!allzero) {
//            System.out.println("Tf idf calculated for " + key + " at round " + round+ " country " + country);
            TopologyHelper.writeToFile(filePath + "/tfidf-" + Long.toString(round) + "-" + country + ".txt",
                    "Key: " + key + ". Tf-idf values: " + tfidfs.toString());
            if(tfidfs.size()<2 || tfidfs.get(tfidfs.size()-2) == 0)
            {
                if(tfidfs.get(tfidfs.size()-1)/0.0001>tfidfEventRate)
                {
                    this.collector.emit(new Values(key, tfidfs, round, country));
                }
            }
            else if(tfidfs.get(tfidfs.size()-1)/tfidfs.get(tfidfs.size()-2)>tfidfEventRate)
            {
                this.collector.emit(new Values(key, tfidfs, round, country));
            }
        }
        else
        {
//            System.out.println("Tf idf all zero for " + key + " at round " + round+ " country " + country);
            TopologyHelper.writeToFile(filePath + "/tfidf-" + Long.toString(round)+"-allzero-" + country + ".txt",
                    "Key: " + key );
        }

    }




    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields( "key", "tfidfs", "round", "country"));
    }
}
