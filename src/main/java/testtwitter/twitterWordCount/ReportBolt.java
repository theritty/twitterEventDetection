package testtwitter.twitterWordCount;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.storm.shade.org.joda.time.DateTime;
import org.apache.storm.shade.org.joda.time.Duration;
import org.apache.storm.shade.org.joda.time.Instant;
import org.apache.storm.shade.org.joda.time.Interval;

import java.io.*;
import java.util.*;

public class ReportBolt extends BaseRichBolt{

    private HashMap<String, Long> counts = new HashMap<>();
    private String fileName;
    private Date lastTime;
    private long writeInterval;

    @Override
    public String toString() {
        return counts.toString();
    }


    ReportBolt(String fileName, long writeIntervalInSeconds)
    {
        this.fileName = fileName;
        this.lastTime = new Date();
        this.writeInterval = writeIntervalInSeconds;
    }

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {

    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.counts.put(word, count);

        Date now = new Date();
        long seconds = (now.getTime()-lastTime.getTime())/1000;

        if(seconds >= writeInterval )
        {
            writeToFile(now);
            lastTime = now;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // this bolt does not emit anything
    }

    public void writeToFile(Date now)
    {
        try {
            PrintWriter writer;
            writer = new PrintWriter(fileName + now.toString() + ".txt");
            write(writer, "----- FINAL COUNTS -----");

            List<Map.Entry<String,Long>> entries = new ArrayList<>(
                    counts.entrySet()
            );
            Collections.sort(
                    entries
                    ,   new Comparator<Map.Entry<String,Long>>() {
                        public int compare(Map.Entry<String,Long> a, Map.Entry<String,Long> b) {
                            return Long.compare(b.getValue(), a.getValue());
                        }
                    }
            );
            for (Map.Entry<String,Long> e : entries) {
                write(writer, e.getKey() + ":" + e.getValue());
            }

            write(writer, "------------------------");

            writer.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void cleanup() {
        writeToFile(new Date());
    }

    public void write(PrintWriter writer, String line) {
        writer.println(line);
        System.out.println(line);
    }
}