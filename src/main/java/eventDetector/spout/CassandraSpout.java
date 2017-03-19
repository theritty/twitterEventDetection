package eventDetector.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import cassandraConnector.CassandraDao;
import eventDetector.drawing.ExcelWriter;
import jnr.ffi.annotations.In;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.util.*;

public class CassandraSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private CassandraDao cassandraDao;
    private ArrayList<Long> roundlist;
    private ArrayList<Long> readRoundlist;
    private int compareSize;
    private int trainSize;
    private int testSize;
    private int componentId;
    private Iterator<Row> iterator = null;
    private long current_round;
    private long count_tweets = 0;
    private String fileNum;
    private boolean start = true;
    private Date startDate = new Date();
    private Date lastDate = new Date();
    private long startRound = 2033719;
    private long endRound = 2033719;
    private int USATask=0;
    private int CANTask=0;

    private HashMap<String, Integer> USAwordsNbolts = new HashMap<>();
    private HashMap<String, Integer> CANwordsNbolts = new HashMap<>();

    private int CANTaskNumber = 0;
    private int USATaskNumber = 0;

    private HashMap<Integer, Long> counts = new HashMap<>();


    public CassandraSpout(CassandraDao cassandraDao, int trainSize, int compareSize, int testSize, String filenum, long start, long end, int canTaskNum, int usaTaskNum) throws Exception {
        this.cassandraDao = cassandraDao;
        this.compareSize = compareSize;
        this.trainSize = trainSize;
        this.testSize = testSize;
        roundlist = new ArrayList<>();
        readRoundlist = new ArrayList<>();
        this.fileNum = filenum + "/";

        this.startRound = start;
        this.endRound = end;
        this.CANTaskNumber = canTaskNum;
        this.USATaskNumber = usaTaskNum;
    }
    @Override
    public void ack(Object msgId) {}
    @Override
    public void close() {}

    @Override
    public void fail(Object msgId) {}

    /**
     * The only thing that the methods will do It is emit each
     * file line
     */
    @Override
    public void nextTuple() {
        /**
         * The nextuple it is called forever, so if we have been readed the file
         * we will wait and then return
         */

        USATask = USATask%USATaskNumber+3;
        CANTask = CANTask%CANTaskNumber+USATaskNumber+3;

        Date nowDate = new Date();
        if(iterator == null || !iterator.hasNext())
        {
            if(roundlist.size()==0)
            {
                try {
//          getEventInfo.report();
                    for(int k=3;k<CANTaskNumber+USATaskNumber+3;k++)
                        collector.emitDirect(k, new Values("dummy", current_round+1, true, new ArrayList<Long>()));

                    try {
                        System.out.println("sleeeeeeeep");
                            Thread.sleep(120000);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    for(int k=3;k<CANTaskNumber+USATaskNumber+3;k++)
                        collector.emitDirect(k, new Values("dummyBLOCKdone", current_round+1, true, new ArrayList<Long>()));

                    System.out.println("Number of tweets: " + count_tweets);
                    Thread.sleep(10000000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return;
            }

            current_round = roundlist.remove(0);
            readRoundlist.add(current_round);
            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + current_round + ".txt",
                    new Date() + ": Round submission from cass spout =>" + current_round );

            if (readRoundlist.size() > compareSize) readRoundlist.remove(0);

            if(start)  start = false;
            startDate = new Date();

            ResultSet resultSet = getDataFromCassandra(current_round);
            iterator = resultSet.iterator();
        }
        Row row = iterator.next();
        String tweet = row.getString("tweet");
        String country = row.getString("country");
        //Date tweetTime = row.getTimestamp("tweettime");
        //long id = row.getLong("id");

        if(tweet == null || tweet.length() == 0) return;
        ArrayList<Long> tmp_roundlist = new ArrayList<>(readRoundlist);

        if(iterator.hasNext()) {
            splitAndEmit(tweet, current_round, tmp_roundlist, country);
        }
        else {
            splitAndEmit(tweet, current_round, tmp_roundlist, country);

            try {
                for(int k=3;k<CANTaskNumber+USATaskNumber+3;k++) {
                    List<Object> values = new ArrayList<>();
                    values.add(current_round);
                    values.add(k-1);
                    values.add(counts.get(k));
                    values.add(0L);
                    values.add(false);
                    if(k<USATaskNumber+3) values.add("USA");
                    else values.add("CAN");
                    cassandraDao.insertIntoProcessed(values.toArray());
                }
                counts.clear();
                CANwordsNbolts.clear();
                USAwordsNbolts.clear();
                for(int k=3;k<CANTaskNumber+USATaskNumber+3;k++)
                    collector.emitDirect(k, new Values("dummy", current_round, true, new ArrayList<Long>()));


                List<Object> values = new ArrayList<>();
                values.add(current_round);
                values.add(12);
                values.add(0L);
                values.add(0L);
                values.add(false);
                values.add("USA");
                cassandraDao.insertIntoProcessed(values.toArray());

                values = new ArrayList<>();
                values.add(current_round);
                values.add(13);
                values.add(0L);
                values.add(0L);
                values.add(false);
                values.add("USA");
                cassandraDao.insertIntoProcessed(values.toArray());


                List<Object> values2 = new ArrayList<>();
                values2.add(current_round);
                values2.add(14);
                values2.add(0L);
                values2.add(0L);
                values2.add(false);
                values2.add("CAN");
                cassandraDao.insertIntoProcessed(values2.toArray());

                values2 = new ArrayList<>();
                values2.add(current_round);
                values2.add(15);
                values2.add(0L);
                values2.add(0L);
                values2.add(false);
                values2.add("CAN");
                cassandraDao.insertIntoProcessed(values2.toArray());


                TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "workhistory.txt", new Date() + " Cass sleeping " + current_round);
                while(true) {
                    Iterator<Row> iteratorProcessed = cassandraDao.getAllProcessed(current_round).iterator();
                    boolean fin = true;
                    while (iteratorProcessed.hasNext()) {
                        if(!iteratorProcessed.next().getBool("finished")) fin = false;
                    }
                    if(fin) break;
                    else Thread.sleep(2000);
                }
                TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "workhistory.txt", new Date() + " Cass wake up " + current_round);


                TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + current_round + ".txt",
                        new Date() + ": Round end from cass spout =>" + current_round );
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + current_round + ".txt",
                    new Date() + ": Round end from cass spout =>" + current_round );

        }
        lastDate = new Date();

        if(!start )
            ExcelWriter.putData(componentId,nowDate,lastDate, "cassSpout", "both", current_round);


        try {
//                TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "workhistory.txt", new Date() + " Cass sleeping " + current_round);
            Thread.sleep(5);
//                TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "workhistory.txt", new Date() + " Cass wake up " + current_round);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void splitAndEmit(String tweetSentence, long round, ArrayList<Long> roundlist, String country) {
        List<String> tweets = Arrays.asList(tweetSentence.split(" "));
        for (String tweet : tweets) {
            if (!tweet.equals("") && tweet.length() > 2 && !tweet.equals("hiring") && !tweet.equals("careerarc")) {
                int taskNum;
                if(country.equals("USA")) {
                    if(USAwordsNbolts.get(tweet)!= null)
                        taskNum = USAwordsNbolts.get(tweet);
                    else
                        taskNum = USATask++;

                    this.collector.emitDirect(taskNum, new Values(tweet, round, false, roundlist));
                    if (counts.get(taskNum) != null)
                        counts.put(taskNum, counts.get(taskNum) + 1);
                    else
                        counts.put(taskNum,1L);
                }
                else {
                    if(CANwordsNbolts.get(tweet)!= null)
                        taskNum = CANwordsNbolts.get(tweet);
                    else
                        taskNum = CANTask++;

                    this.collector.emitDirect(taskNum, new Values(tweet, round, false, roundlist));
                    if (counts.get(taskNum) != null)
                        counts.put(taskNum, counts.get(taskNum) + 1);
                    else
                        counts.put(taskNum,1L);
                }

            }
        }
    }
    public void getRoundListFromCassandra(){
        ResultSet resultSet;
        try {
            resultSet = cassandraDao.getRounds();
            roundlist = new ArrayList<>();

            Iterator<Row> iterator = resultSet.iterator();
            while(iterator.hasNext())
            {
                Row row = iterator.next();
                roundlist.add(row.getLong("round"));
            }
            Collections.sort(roundlist, new Comparator<Long>() {
                public int compare(Long m1, Long m2) {
                    return m1.compareTo(m2);
                }
            });

            System.out.println(roundlist);

            while(roundlist.get(0)<startRound)
                roundlist.remove(0);
            while(roundlist.get(roundlist.size()-1)>endRound)
                roundlist.remove(roundlist.size()-1);

            System.out.println(roundlist);

            readRoundlist.add(roundlist.get(0)-6);
            readRoundlist.add(roundlist.get(0)-4);
            readRoundlist.add(roundlist.get(0)-2);





        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public ResultSet getDataFromCassandra(long round) {
        ResultSet resultSet = null;
        try {
            resultSet = cassandraDao.getTweetsByRound(round);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return resultSet;
    }

    /**
     * We will create the file and get the collector object
     */
    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        getRoundListFromCassandra();
        this.collector = collector;
        this.componentId = context.getThisTaskId()-1;
        System.out.println("cas: s" + " id: " + componentId);
        this.startRound = roundlist.get(0);
        ExcelWriter.putStartDate(new Date(), fileNum, this.startRound);
    }

    /**
     * Declare the output field "word"
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare( new Fields("word", "round", "blockEnd", "dates"));
//        declarer.declareStream("USA", new Fields("word", "round", "blockEnd", "dates"));
//        declarer.declareStream("CAN", new Fields("word", "round", "blockEnd", "dates"));
    }

}