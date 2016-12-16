package eventDetector.bolts;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

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
    private HashMap<Long, Long> ignores;
    private String componentId;
    private String fileNum;
    private Date lastDate = new Date();
    private Date startDate = new Date();

    public EventDetectorManagementBolt(String filePath, String fileNum)
    {
        this.filePath = filePath + fileNum + "/" ;
        words = new ArrayList<>();
        ignores = new HashMap<>();
        this.fileNum = fileNum +"/";
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

        TopologyHelper.writeToFile("/Users/ozlemcerensahin/Desktop/workhistory.txt", new Date() + " Mgmt detector " + componentId + " working " + round);
        if(round < currentRound)
        {
            ignores.putIfAbsent(round, 0L);
            ignores.put(round,ignores.get(round)+1);

            ignoredCount++;
            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + "ignoreCount.txt",
                    "Management bolt Ignoring " + word + " from round " + round +
                            " while evaluating round " + currentRound + ". total ignore count: " + ignoredCount);

            for(long r:ignores.keySet())
                TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + "ignoreCount.txt",
                        "Management bolt Ignored count " + componentId + " : " + ignoredCount + " round " + r +
                                " ignore count: " + ignores.get(r));

            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + "ignoreCount.txt",
                    "---------------------------------------------------------------------------------");
            return;
        }

        if(round > currentRound)
        {
            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
                    "Management bolt " + componentId + " end of round " + currentRound + " at " + lastDate );

            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
                    "Word count "+ componentId + " time taken for round" + currentRound + " is " +
                            (lastDate.getTime()-startDate.getTime())/1000);

            startDate = new Date();
            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + round + ".txt",
                    "Management bolt " + componentId + " start of round " + round + " at " + new Date() );

            writeToFile(country, currentRound);
            endOfRoundOperations(currentRound, country, rounds);
            currentRound = round;
            rounds = (ArrayList<Long>)tuple.getValueByField("dates");
            words.clear();
        }
        lastDate = new Date();
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
        PrintWriter writer1 = null, writer2= null;
        try {
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

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            writer1.close();
            writer2.close();
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