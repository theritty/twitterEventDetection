package eventDetector.bolts;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cassandraConnector.CassandraDao;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

public class EventDetectorManagementBolt extends BaseRichBolt{
    private HashMap<Long, RoundInfo> roundInfoList;
    private OutputCollector collector;
    private CassandraDao cassandraDao;
    private String filePath;
    private ArrayList<Long> finishedRounds;
    private long ignoredCount = 0;

    public EventDetectorManagementBolt(CassandraDao cassandraDao, String filePath, String fileNum)
    {
        this.filePath = filePath + fileNum + "/" ;
        roundInfoList = new HashMap<>();
        this.cassandraDao = cassandraDao;
        finishedRounds = new ArrayList<>();
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

//        System.out.println("Manager: word " + word + " country: " + country + " round: " + round + " count: " + count + " inpBolt: " + inputBolt );
        RoundInfo roundInfo;
        if(finishedRounds.contains(round))
        {
            ignoredCount++;
            if(ignoredCount%1000==0)
                System.out.println("Ignored count: " + ignoredCount);
            return;
        }
        else if(roundInfoList.get(round) != null)
        {
            roundInfo = roundInfoList.get(round);
        }
        else
        {
            roundInfo = new RoundInfo();
            roundInfoList.put(round, roundInfo);
        }

//        if(roundInfo.isEndOfRound()) return;

//        if(country.equals("CAN"))
//            System.out.println("Round " + round + " word " + word + " for CANADA");
//        else
//            System.out.println("hof");
        if(inputBolt.equals("WordCount"))
        {

//            System.out.println("Manager word: " + word + " country: " + country + " round: " + round + " count: " + count );

            if(blockEnd)
            {
//                System.out.println("Manager blockend word round: " + round );
                if( roundInfo.getWordCounts().size()>0) {
                    if (roundInfoList.get(round) != null)
                        writeToFile(country, round, roundInfoList.get(round).getWordCounts(), "sentences");
                }
                roundInfo.setWordBlockEnd();
            }
            else
            {
                roundInfo.putWord(word, count);
            }
        }
        else if(inputBolt.equals("HashtagCount"))
        {

//            System.out.println("Manager hashtag: " + word + " country: " + country + " round: " + round + " count: " + count );

            if(blockEnd )
            {
//                System.out.println("Manager blockend hashtag round: " + round );
                if(roundInfo.getHashtagCounts().size()>0) {
                    if (roundInfoList.get(round) != null)
                        writeToFile(country, round, roundInfoList.get(round).getHashtagCounts(), "hashtags");
                }
                roundInfo.setHashtagBlockEnd();
            }
            else
            {
                roundInfo.putHashtag(word, count);
            }
        }
        roundInfoList.put(round, roundInfo);


//        if(word.equals("BLOCKEND") && round==2033805){
//            System.out.println("here");
//        }
        if(roundInfo.isEndOfRound())
        {
//            System.out.println("Manager: End of round " + round + " for country " + country );
            ArrayList<Long> rounds = (ArrayList<Long>)tuple.getValueByField("dates");

            if(roundInfoList.get(round)!=null) {
                endOfRoundOperations(roundInfoList.get(round).getWordCounts(), round, country, source, rounds, "word");
                endOfRoundOperations(roundInfoList.get(round).getHashtagCounts(), round, country, source, rounds, "hashtag");
            }
            else
            {
//                System.out.println("Manager: End of round " + round + " for country " + country + ". No entry for round! " );
            }
            System.out.println(new Date() + ": TF-IDF calculation and event detection step is starting for round => " + round);
            roundInfoList.remove(round);
        }
    }

    public void endOfRoundOperations(HashMap<String, Long> countlist, long round, String country, String source, ArrayList<Long> rounds, String type)
    {
        if(countlist!=null && countlist.size()>0) {
            for (Map.Entry<String, Long> w : countlist.entrySet()) {
                if (w != null) {
                    insertValuesToCass(round, w.getKey(), country, w.getValue());
                    //new Fields("rounds", "key", "type", "round", "source", "country"));
                    this.collector.emit(new Values(rounds, w.getKey(), type, round, source, country));
                }
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