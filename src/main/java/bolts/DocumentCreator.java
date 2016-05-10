package bolts;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.*;
import java.util.*;

public class DocumentCreator extends BaseRichBolt{

    private OutputCollector collector;
    private int fileNum;
    private boolean active = true;
    private String oldFile="";

    public DocumentCreator(int fileNum, boolean active)
    {
        this.fileNum = fileNum;
        this.active  = active;
    }

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        ArrayList<Date> dates = (ArrayList<Date>)tuple.getValueByField("dates");
        Date currentDate = (Date) tuple.getValueByField("currentDate");
        String fileName = currentDate.toString() + ".txt";
        List<String> tweets = (List<String>) tuple.getValueByField( "tweet" );
        long round = tuple.getLongByField("round");

        String tweet = "";// tweets.toString(); //tweet_pre.getText().toLowerCase();
        Boolean blockEnd = (Boolean) tuple.getValueByField("blockEnd");

        if(active) {
            for (String twee : tweets) {
                tweet += twee + " ";
            }

//        System.out.println("Writing to file " + fileName);
//            System.out.println("Doc:::  filename " + fileName);
            if(!oldFile.equals(fileName))
            {
                System.out.println("new Tweet Doc created: " + fileName);
                oldFile = fileName;
            }
            writeToFile(fileName, tweet);
        }

        if(blockEnd){
            System.out.println("Doc::: current " + currentDate + " dates " + dates + " filename " + fileName);
            this.collector.emit(new Values(dates, blockEnd, "DocumentCreator", round));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("dates", "blockEnd", "inputBolt", "round"));
    }

    public void writeToFile(String fileName, String tweet)
    {
        try {
            PrintWriter writer = new PrintWriter(new FileOutputStream(
                    new File(fileNum + "/" + fileName),
                    true /* append = true */));

            write(writer, tweet);
            writer.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void cleanup() {

    }

    public void write(PrintWriter writer, String line) {
        writer.println(line);
//        System.out.println(line);
    }
}