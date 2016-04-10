package testtwitter.twitterWordCount;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.storm.shade.org.joda.time.DateTime;
import org.apache.storm.shade.org.joda.time.Duration;
import org.apache.storm.shade.org.joda.time.Instant;
import org.apache.storm.shade.org.joda.time.Interval;
import twitter4j.Status;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class DocumentCreator extends BaseRichBolt{

    private OutputCollector collector;

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        ArrayList<Date> dates = (ArrayList<Date>)tuple.getValueByField("dates");
        String fileName = dates.get(dates.size()-1).toString() + ".txt";
        Status tweet_pre = (Status) tuple.getValueByField( "tweet" );
        long round = tuple.getLongByField("round");
        String tweet = tweet_pre.getText().toLowerCase();
        Boolean blockEnd = (Boolean) tuple.getValueByField("blockEnd");

//        System.out.println("Writing to file " + fileName);
        writeToFile(fileName, tweet);
        if(blockEnd){
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
                    new File(fileName),
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