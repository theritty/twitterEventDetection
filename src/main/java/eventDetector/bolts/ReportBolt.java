package eventDetector.bolts;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

public class ReportBolt extends BaseRichBolt{
    private HashMap<Long, RoundInfo> roundInfoListUSA;
    private HashMap<Long, RoundInfo> roundInfoListCAN;
    private String filePath;
    static Logger log = LoggerFactory.getLogger(ReportBolt.class);

    public ReportBolt(String fileName, int threshold, String filePath, int fileNum)
    {
        this.filePath = filePath + fileNum + "/" ;
        roundInfoListUSA = new HashMap<>();
        roundInfoListCAN = new HashMap<>();
    }

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {

    }

    @Override
    public void execute(Tuple tuple) {
        String inputBolt = tuple.getStringByField( "inputBolt" );
        String word = tuple.getStringByField("word");
        String country = tuple.getStringByField("country");
        long count = tuple.getLongByField("count");
        long round = tuple.getLongByField("round");
        Boolean blockEnd = (Boolean) tuple.getValueByField("blockEnd");

        log.debug("Word: " + word + " round: " + round + "country: "+ country + " inputBolt: " + inputBolt);

        HashMap<Long, RoundInfo> roundInfoListTmp;
        if(country.equals("USA")) roundInfoListTmp = roundInfoListUSA;
        else roundInfoListTmp = roundInfoListCAN;

        RoundInfo roundInfo;
        if(roundInfoListTmp.get(round) != null)
        {
            roundInfo = roundInfoListTmp.get(round);
        }
        else
        {
            roundInfo = new RoundInfo();
            roundInfoListTmp.put(round, roundInfo);
        }

//        if(country.equals("CAN"))
//            System.out.println("Round " + round + " word " + word + " for CANADA");
//        else
//            System.out.println("hof");
        if(inputBolt.equals("WordCount"))
        {
            log.debug("Set as sentence:: Word: " + word + " round: " + round + "country: "+ country + " inputBolt: " + inputBolt);
            if(blockEnd && roundInfo.getWordCounts().size()>0)
            {
                if(roundInfoListUSA.get(round)!=null)
                    writeToFile("USA", round, roundInfoListUSA.get(round).getWordCounts(), "sentences");
                if(roundInfoListCAN.get(round)!=null)
                    writeToFile("CAN", round, roundInfoListCAN.get(round).getWordCounts(), "sentences");
            }
            else
            {
                roundInfo.putWord(word, count);
            }
        }
        else if(inputBolt.equals("HashtagCount"))
        {
            log.debug("Set as hashtag:: Word: " + word + " round: " + round + "country: "+ country + " inputBolt: " + inputBolt);
            if(blockEnd && roundInfo.getHashtagCounts().size()>0)
            {
                if(roundInfoListUSA.get(round)!=null)
                    writeToFile("USA", round, roundInfoListUSA.get(round).getHashtagCounts(), "hashtags");
                if(roundInfoListCAN.get(round)!=null)
                    writeToFile("CAN", round, roundInfoListCAN.get(round).getHashtagCounts(), "hashtags");
            }
            else
            {
                roundInfo.putHashtag(word, count);
            }
        }
        else
        {
            log.debug("Cannot set:: Word: " + word + " round: " + round + "country: "+ country + " inputBolt: " + inputBolt);
        }
//        System.out.println("Round: " + round + " ----------------------------------------------");
//        if(roundInfoListCAN.get(round)!=null) {
//            System.out.println("Canada hashtag: " + roundInfoListCAN.get(round).getHashtagCounts().size());
//            System.out.println("Canada word: " + roundInfoListCAN.get(round).getWordCounts().size());
//        }
//        if(roundInfoListUSA.get(round)!=null) {
//            System.out.println("USA hashtag: " + roundInfoListUSA.get(round).getHashtagCounts().size());
//            System.out.println("USA word: " + roundInfoListUSA.get(round).getWordCounts().size());
//        }
//        System.out.println("-------------------------------------------------------------");

        if(country.equals("USA")) roundInfoListUSA = roundInfoListTmp;
        else    roundInfoListCAN=roundInfoListTmp;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // this bolt does not emit anything
    }

    public void writeToFile(String country, long round, HashMap<String, Long> countList, String fileName)
    {
        try {
            PrintWriter writer;
            writer = new PrintWriter(filePath + fileName + Long.toString(round) + "-" + country + ".txt");
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