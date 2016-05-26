package bolts.CassandraSpoutProcessing;

import algorithms.TFIDFCalculator;
import algorithms.TFIDFCalculatorWithCassandra;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cass.CassandraDao;
import topologies.topologyBuild.Constants;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Map;

public class EventDetectorWithCassandraBolt extends BaseRichBolt {

    private OutputCollector collector;
    private String filePath;
    private int inputFileNum;
    private double tfidfEventRate;
    private CassandraDao cassandraDao;

    public EventDetectorWithCassandraBolt(String filePath, int fileNum, double tfidfEventRate, int inputFileNum, CassandraDao cassandraDao)
    {
        this.tfidfEventRate = tfidfEventRate;
        this.filePath = filePath + fileNum;
        this.inputFileNum = inputFileNum;
        this.cassandraDao = cassandraDao;
    }

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {

        ArrayList<Long> rounds = (ArrayList<Long>)tuple.getValueByField("rounds");
        String key = tuple.getStringByField("key");
        String type = tuple.getStringByField("type");
        long round = tuple.getLongByField("round");
        String source = (String) tuple.getValueByField( "source" );
        String country = (String) tuple.getValueByField( "country" );

//      System.out.println(round + " " + key + " here1");
        ArrayList<Double> tfidfs = new ArrayList<>();

        for (long roundNum: rounds)
        {
            TFIDFCalculatorWithCassandra calculator = new TFIDFCalculatorWithCassandra();
            if(source.equals("twitter"))
            {
                tfidfs.add(calculator.tfIdf( rounds,key,roundNum,cassandraDao));
            }
            else
            {
                tfidfs.add(calculator.tfIdf(rounds,key,roundNum,cassandraDao));
            }
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
            writeToFile(filePath + "/tfidf-" + Long.toString(round) + "-" + country + ".txt", "Key: " + key + ". Tf-idf values: " + tfidfs.toString());
            if(tfidfs.get(tfidfs.size()-2) == 0 && tfidfs.get(tfidfs.size()-1)/0.0001>tfidfEventRate)
            {
              System.out.println("Round " + round + " Event candidate: " + key+ " rate: "
                      + tfidfs.get(tfidfs.size()-1)/0.0001);
              this.collector.emit(new Values(key, tfidfs, type, round, source, country));
            }
            else if(tfidfs.get(tfidfs.size()-1)/tfidfs.get(tfidfs.size()-2)>tfidfEventRate)
            {
              System.out.println("Round " + round + " Event candidate: " + key+ " rate: "
                      + tfidfs.get(tfidfs.size()-1)/tfidfs.get(tfidfs.size()-2));
              this.collector.emit(new Values(key, tfidfs, type, round, source, country));
            }
          else
            {
              if(tfidfs.get(tfidfs.size()-2) == 0)
                System.out.println("Round " + round + " Not Event " + key+ " rate: "
                      + tfidfs.get(tfidfs.size()-1)/0.0001);
              else
                System.out.println("Round " + round + " Not Event " + key+ " rate: "
                        + tfidfs.get(tfidfs.size()-1)/tfidfs.get(tfidfs.size()-2));
            }
        }
        else
            writeToFile(filePath + "/tfidf-" + Long.toString(round)+"-allzero-" + country + ".txt", "Key: " + key );
//      System.out.println(round + " " + key + " here2 " );
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
        declarer.declare(new Fields( "key", "tfidfs", "type", "round", "source", "country"));
    }
}
