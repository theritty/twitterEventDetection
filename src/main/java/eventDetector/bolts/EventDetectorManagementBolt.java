package eventDetector.bolts;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

public class EventDetectorManagementBolt extends BaseRichBolt{
    private OutputCollector collector;
    private String filePath;
    private ArrayList<String> words;
    private long ignoredCount = 0;
    private long currentRound = 0;
    private ArrayList<Long> rounds;
    private String componentId;

    public EventDetectorManagementBolt(String filePath, String fileNum)
    {
        this.filePath = filePath + fileNum + "/" ;
        words = new ArrayList<>();
    }

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.componentId = String.valueOf(UUID.randomUUID());
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        String country = tuple.getStringByField("country");
        long round = tuple.getLongByField("round");

        if(round < currentRound)
        {
            ignoredCount++;
            if(ignoredCount%1000==0)
                System.out.println("Management bolt Ignored count " + componentId +" : " + ignoredCount);
            return;
        }

        if(round > currentRound)
        {
            System.out.println("Management bolt " + componentId + " end of round " + currentRound + " at " + new Date());

            writeToFile(country, currentRound);
            endOfRoundOperations(currentRound, country, rounds);
            currentRound = round;
            rounds = (ArrayList<Long>)tuple.getValueByField("dates");
            words.clear();
        }
        words.add(word);
    }

    public void endOfRoundOperations(long round, String country, ArrayList<Long> rounds)
    {
        for (String word: words)
            this.collector.emit(new Values(rounds, word, round, country));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("rounds", "key","round", "country"));
    }

    public void writeToFile(String country, long round)
    {
        try {
            PrintWriter writer1, writer2;
            writer1 = new PrintWriter(filePath + "word" + Long.toString(round) + "-" + country + ".txt");
            writer2 = new PrintWriter(filePath + "hashtag" + Long.toString(round) + "-" + country + ".txt");
            write(writer1, "----- FINAL COUNTS -----");
            write(writer2, "----- FINAL COUNTS -----");

            for (String word : words) {
                if(!word.startsWith("#"))   write(writer1, word);
                else                        write(writer2, word);
            }

            write(writer1, "------------------------");
            write(writer2, "------------------------");
            writer1.close();
            writer2.close();

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