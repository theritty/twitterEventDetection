package bolts.CommonBolts;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.io.*;
import java.util.*;

public class ReportBolt extends BaseRichBolt{
    private HashMap<Long, RoundInfo> roundInfoList;
    private String filePath;

    public ReportBolt(String fileName, int threshold, String filePath, int fileNum)
    {
        this.filePath = filePath + fileNum + "/" + fileName;
        roundInfoList = new HashMap<>();
    }

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {

    }

    @Override
    public void execute(Tuple tuple) {
        String inputBolt = tuple.getStringByField( "inputBolt" );
        long round = tuple.getLongByField("round");
        String word = tuple.getStringByField("word");
        String country = tuple.getStringByField("country");
        Long count = tuple.getLongByField("count");
        Boolean blockEnd = (Boolean) tuple.getValueByField("blockEnd");

//        System.out.println("Report BOLT");
        RoundInfo roundInfo;
        if(roundInfoList.get(round) != null)
        {
            roundInfo = roundInfoList.get(round);
        }
        else
        {
            roundInfo = new RoundInfo();
            roundInfoList.put(round, roundInfo);
        }

        if(inputBolt.equals("WordCount"))
        {
            if(blockEnd && roundInfo.getWordCounts().size()>0)
            {
                writeToFile(country, round, roundInfo.getWordCounts());
            }
            else
            {
                roundInfo.putWord(word, count);
            }
        }
        else if(inputBolt.equals("HashtagCount"))
        {
            if(blockEnd && roundInfo.getWordCounts().size()>0)
            {
                writeToFile(country, round, roundInfo.getHashtagCounts());
            }
            else
            {
                roundInfo.putHashtag(word, count);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // this bolt does not emit anything
    }

    public void writeToFile(String country, long round, HashMap<String, Long> countList)
    {
        try {
            PrintWriter writer;
            writer = new PrintWriter(filePath + Long.toString(round) + "-" + country + ".txt");
            write(writer, "----- FINAL COUNTS -----");

            List<Map.Entry<String,Long>> entries = new ArrayList<>(
                    countList.entrySet()
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