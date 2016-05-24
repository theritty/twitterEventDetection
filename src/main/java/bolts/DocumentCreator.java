package bolts;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.GeoLocation;

import java.io.*;
import java.util.*;

public class DocumentCreator extends BaseRichBolt{

    private OutputCollector collector;
    private String fileLocation;
    private boolean active = true;
    private String oldFile="";

    public DocumentCreator(String prefix, int fileNum, boolean active)
    {
        this.fileLocation = prefix + fileNum;
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
        String source = (String) tuple.getValueByField( "source" );
        GeoLocation[][] loc = (GeoLocation[][]) tuple.getValueByField("location");

        String tweet = "";
        Boolean blockEnd = (Boolean) tuple.getValueByField("blockEnd");
        if(loc == null) return;

        if(active) {
            for (String twee : tweets) {
                tweet += twee + " ";
            }

            if(!oldFile.equals(fileName))
            {
                System.out.println("new Tweet Doc created: " + fileName);
                oldFile = fileName;
            }

          double latitude = loc[0][0].getLatitude();
          if(latitude>31 && latitude<45)
            writeToFile(fileName.substring(0,fileName.length()-4) + "-USA.txt", tweet);
          else
            writeToFile(fileName.substring(0,fileName.length()-4) + "-CAN.txt", tweet);
        }

        if(blockEnd){
            System.out.println("Doc::: current " + currentDate + " dates " + dates + " filename " + fileName);
            this.collector.emit(new Values(dates, blockEnd, "DocumentCreator", round, source));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("dates", "blockEnd", "inputBolt", "round", "source"));
    }

    public void writeToFile(String fileName, String tweet)
    {
        try {
            PrintWriter writer = new PrintWriter(new FileOutputStream(
                    new File(fileLocation + "/" + fileName),
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