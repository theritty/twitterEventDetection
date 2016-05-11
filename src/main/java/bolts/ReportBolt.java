package bolts;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.io.*;
import java.util.*;

public class ReportBolt extends BaseRichBolt{

    private HashMap<String, Long> counts = new HashMap<>();
    private String filePath;
    private Date lastTime;
    private long round;
    private int threshold;

    @Override
    public String toString() {
        return counts.toString();
    }


    public ReportBolt(String fileName, int threshold, String filePath, int fileNum)
    {
        this.filePath = filePath + fileNum + "/" + fileName;
        this.lastTime = new Date();
        this.threshold = threshold;
        this.round = 0;
    }

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {

    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        long round = tuple.getLongByField("round");
        this.counts.put(word, count);

        if(this.round < round)
        {
//            System.out.println("New count report: " + filePath + Long.toString(round));
            writeToFile(this.round);
            this.round = round;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // this bolt does not emit anything
    }

    public void writeToFile(long round)
    {
        try {
            PrintWriter writer;
            writer = new PrintWriter(filePath + Long.toString(round) + ".txt");
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
                if(e.getValue() >= 0/*threshold*/) {
                    write(writer, e.getKey() + ":" + e.getValue());
                }
            }

            write(writer, "------------------------");

            writer.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void cleanup() {
        //writeToFile(new Date());
    }

    public void write(PrintWriter writer, String line) {
        writer.println(line);
//        System.out.println(line);
    }
}