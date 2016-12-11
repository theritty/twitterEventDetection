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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
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

    public EventDetectorWithCassandraBolt(CassandraDao cassandraDao, String filePath, String fileNum, double tfidfEventRate, String tweetTable )
    {
        this.tfidfEventRate = tfidfEventRate;
        this.filePath = filePath + fileNum;
        this.cassandraDao = cassandraDao;
        this.tweetTable = tweetTable;
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


//        System.out.println("Event Detector Bolt for " + key + " at round " + round+ " country " + country);
        ArrayList<Double> tfidfs = new ArrayList<>();
        System.out.println("Detector bolt " + componentId + " start of round " + round + " at " + new Date());

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
            writeToFile(filePath + "/tfidf-" + Long.toString(round) + "-" + country + ".txt", "Key: " + key + ". Tf-idf values: " + tfidfs.toString());
            if(tfidfs.size()<2 || tfidfs.get(tfidfs.size()-2) == 0)
            {
                System.out.println("tfidf for " + key + " is " + tfidfs.get(tfidfs.size()-1)/0.0001 );
                if(tfidfs.get(tfidfs.size()-1)/0.0001>tfidfEventRate)
                {
                    this.collector.emit(new Values(key, tfidfs, round, country));
                }
            }
            else if(tfidfs.get(tfidfs.size()-1)/tfidfs.get(tfidfs.size()-2)>tfidfEventRate)
            {
                System.out.println("tfidf for " + key + " is " + tfidfs.get(tfidfs.size()-1)/tfidfs.get(tfidfs.size()-2) );
                this.collector.emit(new Values(key, tfidfs, round, country));
            }
        }
        else
        {
//            System.out.println("Tf idf all zero for " + key + " at round " + round+ " country " + country);
            writeToFile(filePath + "/tfidf-" + Long.toString(round)+"-allzero-" + country + ".txt", "Key: " + key );
        }

        System.out.println("Detector bolt " + componentId + " end of round " + round + " at " + new Date());
    }

    public void writeToFile(String fileName, String tweet)
    {
        try {
            PrintWriter writer = new PrintWriter(new FileOutputStream(
                    new File(fileName),
                    true /* append = true */));

            write(writer, tweet);
            writer.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void write(PrintWriter writer, String line) {
        writer.println(line);
//        System.out.println(line);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields( "key", "tfidfs", "round", "country"));
    }
}
