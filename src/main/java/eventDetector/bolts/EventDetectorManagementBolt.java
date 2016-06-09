package eventDetector.bolts;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cassandraConnector.CassandraDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

public class EventDetectorManagementBolt extends BaseRichBolt{
    private HashMap<Long, RoundInfo> roundInfoListUSA;
    private HashMap<Long, RoundInfo> roundInfoListCAN;
    private OutputCollector collector;
    private CassandraDao cassandraDao;
    private String filePath;

    public EventDetectorManagementBolt(CassandraDao cassandraDao, String filePath, int fileNum)
    {
        this.filePath = filePath + fileNum + "/" ;
        roundInfoListUSA = new HashMap<>();
        roundInfoListCAN = new HashMap<>();
        this.cassandraDao = cassandraDao;
    }

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String inputBolt = tuple.getStringByField( "inputBolt" );
        String word = tuple.getStringByField("word");
        String country = tuple.getStringByField("country");
        String source = (String) tuple.getValueByField( "source" );
        long count = tuple.getLongByField("count");
        long round = tuple.getLongByField("round");
        Boolean blockEnd = (Boolean) tuple.getValueByField("blockEnd");


//        System.out.println("Mng: " + word);

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

        if(roundInfo.isEndOfRound()) return;

//        if(country.equals("CAN"))
//            System.out.println("Round " + round + " word " + word + " for CANADA");
//        else
//            System.out.println("hof");
        if(inputBolt.equals("WordCount"))
        {
            if(blockEnd && roundInfo.getWordCounts().size()>0)
            {
                if(roundInfoListUSA.get(round)!=null)
                    writeToFile("USA", round, roundInfoListUSA.get(round).getWordCounts(), "sentences");
                if(roundInfoListCAN.get(round)!=null)
                    writeToFile("CAN", round, roundInfoListCAN.get(round).getWordCounts(), "sentences");
                roundInfo.setWordBlockEnd();
            }
            else
            {
                roundInfo.putWord(word, count);
            }
        }
        else if(inputBolt.equals("HashtagCount"))
        {
            if(blockEnd && roundInfo.getHashtagCounts().size()>0)
            {
                if(roundInfoListUSA.get(round)!=null)
                    writeToFile("USA", round, roundInfoListUSA.get(round).getHashtagCounts(), "hashtags");
                if(roundInfoListCAN.get(round)!=null)
                    writeToFile("CAN", round, roundInfoListCAN.get(round).getHashtagCounts(), "hashtags");
                roundInfo.setHashtagBlockEnd();
            }
            else
            {
                roundInfo.putHashtag(word, count);
            }
        }

        roundInfoListTmp.put(round, roundInfo);

        if(country.equals("USA"))
            roundInfoListUSA = roundInfoListTmp;
        else
            roundInfoListCAN = roundInfoListTmp;


        if(roundInfo.isEndOfRound())
        {
            ArrayList<Long> rounds = (ArrayList<Long>)tuple.getValueByField("dates");

            endOfRoundOperations(roundInfoListUSA.get(round).getWordCounts(), round, "USA", source, rounds,"word");
            endOfRoundOperations(roundInfoListUSA.get(round).getHashtagCounts(), round, "USA", source, rounds,"hashtag");
            endOfRoundOperations(roundInfoListCAN.get(round).getWordCounts(), round, "CAN", source, rounds, "word");
            endOfRoundOperations(roundInfoListCAN.get(round).getHashtagCounts(), round, "CAN", source, rounds, "hashtag");
        }

    }

    public void endOfRoundOperations(HashMap<String, Long> countlist, long round, String country, String source, ArrayList<Long> rounds, String type)
    {
        for(Map.Entry<String,Long> w : countlist.entrySet())
        {
            if(w != null) {
                insertValuesToCass(round, w.getKey(), country, w.getValue());
                this.collector.emit(new Values(rounds, w.getKey(), type, round, source, country));
            }
        }
    }

    private void insertValuesToCass(long round, String word, String country, long count)
    {
        try {
            List<Object> values = new ArrayList<>();
            values.add(round);
            values.add(word);
            values.add(country);
            values.add(count);
            values.add(-1L);
            cassandraDao.insertIntoCounts(values.toArray());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("rounds", "key", "type", "round", "source", "country"));
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