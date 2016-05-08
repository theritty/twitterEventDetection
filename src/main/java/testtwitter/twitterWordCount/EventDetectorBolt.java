package testtwitter.twitterWordCount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.*;

public class EventDetectorBolt extends BaseRichBolt {

    private OutputCollector collector;
    private int fileNum;

    EventDetectorBolt(int fileNum)
    {
        this.fileNum = fileNum;
    }

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {

        ArrayList<String> dates = (ArrayList<String>)tuple.getValueByField("dates");
        String key = tuple.getStringByField("key");
        String type = tuple.getStringByField("type");
        long round = tuple.getLongByField("round");

        ArrayList<Double> tfidfs = new ArrayList<>();


        System.out.println("detect::: dates " + dates);

        for (String date: dates)
        {
            TFIDFCalculator calculator = new TFIDFCalculator();
//            tfidfs.add(calculator.tfIdf(Integer.toString(fileNum), dates,key,date));
            tfidfs.add(calculator.tfIdf("tweets", dates,key,date));
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
        if(!allzero)
            writeToFile(fileNum + "/tfidf-" + Long.toString(round)+".txt", "Key: " + key + ". Tf-idf values: " + tfidfs.toString());
        else
            writeToFile(fileNum + "/tfidf-" + Long.toString(round)+"-allzero.txt", "Key: " + key );
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
        //declarer.declare(new Fields("word"));
    }
}
