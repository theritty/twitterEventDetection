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
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.util.*;

public class CassandraSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private CassandraDao cassandraDao;
    private ArrayList<Long> roundlist;
    private int componentId;
    private Iterator<Row> iterator = null;
    private long current_round;
    private long count_tweets = 0;
    private String fileNum;
    private boolean start = true;
    private Date startDate = new Date();
    private Date lastDate = new Date();
    private long startRound = 0;

    private int USATaskNumber ;
    private int CANTaskNumber ;

    private int numWorkers ;
    private int numDetectors ;


    private int USATask = 0;
    private int CANTask = 0;

    private HashMap<String, Integer> USAwordMap = new HashMap<>();
    private HashMap<String, Integer> CANwordMap = new HashMap<>();


    public CassandraSpout(CassandraDao cassandraDao, String filenum, int USATaskNumber, int CANTaskNumber, int numWorkers, int numDetectors) throws Exception {
        this.cassandraDao = cassandraDao;
        this.roundlist = new ArrayList<>();
        this.fileNum = filenum + "/";
        this.USATaskNumber = USATaskNumber;
        this.CANTaskNumber = CANTaskNumber;
        this.numWorkers = numWorkers;
        this.numDetectors = numDetectors;

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

        if(iterator == null || !iterator.hasNext())
        {
            if(roundlist.size()==0)
            {
//          getEventInfo.report();
                System.out.println("Number of tweets: " + count_tweets);
                try {
                    System.out.println("sleeeeeeeep");
                    Thread.sleep(1800000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return;
            }

            ExcelWriter.putData(componentId,startDate,lastDate, "cassSpout", "both", current_round);

            current_round = roundlist.remove(0);
            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + current_round + ".txt",
                    new Date() + ": Round submission from cass spout =>" + current_round );

//            try {
//                if(!start) {
//                    TopologyHelper.writeToFile(Constants.WORKHISTORY_FILE + fileNum+ "workhistory.txt", new Date() + " Cass sleeping " + current_round);
//                    Thread.sleep(10000);
//                    TopologyHelper.writeToFile(Constants.WORKHISTORY_FILE + fileNum+ "workhistory.txt", new Date() + " Cass wake up " + current_round);
//                }
//                else start = false;
//            }
//            catch (Exception e) {
//                e.printStackTrace();
//            }
            startDate = new Date();

            ResultSet resultSet = getDataFromCassandra(current_round);
            iterator = resultSet.iterator();
        }
        Row row = iterator.next();
        String tweet = row.getString("tweet");
        String country = row.getString("country");

        if(tweet == null || tweet.length() == 0) return;

        if(iterator.hasNext()) {
            splitAndEmit(tweet, current_round, country);
        }
        else {
            splitAndEmit(tweet, current_round, country);

            for(int k=2+numWorkers;k<CANTaskNumber+USATaskNumber+2+numWorkers+numDetectors;k++) {
                try {
                    List<Object> values = new ArrayList<>();
                    values.add(current_round);
                    values.add(k-1);
                    values.add(false);
                    cassandraDao.insertIntoProcessed(values.toArray());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            for(int k=2+numWorkers;k<CANTaskNumber+USATaskNumber+2+numWorkers;k++)
                collector.emitDirect(k, new Values("BLOCKEND", current_round, true));

            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + current_round + ".txt",
                    new Date() + ": Round end from cass spout =>" + current_round );


            try {
                TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "workhistory.txt", new Date() + " Cass sleeping " + current_round);
                while (true) {
                    Iterator<Row> iteratorProcessed = cassandraDao.getAllProcessed(current_round).iterator();
                    boolean fin = true;
                    while (iteratorProcessed.hasNext()) {
                        if (!iteratorProcessed.next().getBool("finished")) fin = false;
                    }
                    if (fin) break;
                    else Thread.sleep(2000);
                }
                USAwordMap.clear();
                CANwordMap.clear();
                TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "workhistory.txt", new Date() + " Cass wake up " + current_round);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        lastDate = new Date();

    }

    public void sendToNextTaskNum(String country, String tweet, long round) {
        if(country.equals("USA")) {
            if(USAwordMap.containsKey(tweet)) {
                this.collector.emitDirect(USAwordMap.get(tweet), new Values(tweet, round, false));
            }
            else {
                this.collector.emitDirect(USATask, new Values(tweet, round, false));
                USAwordMap.put(tweet, USATask++);
                if ((USATask < 2 + numWorkers) || (USATask >= USATaskNumber + 2 + numWorkers))
                    USATask = 2 + numWorkers;
            }
        }
        else {
            if(CANwordMap.containsKey(tweet)) {
                this.collector.emitDirect(CANwordMap.get(tweet), new Values(tweet, round, false));
            }
            else {
                this.collector.emitDirect(CANTask, new Values(tweet, round, false));
                CANwordMap.put(tweet, CANTask++);
                if ((CANTask < USATaskNumber + 2 + numWorkers) || (CANTask >= CANTaskNumber + USATaskNumber + 2 + numWorkers))
                    CANTask = USATaskNumber + 2 + numWorkers;
            }
        }
    }

    public void splitAndEmit(String tweetSentence, long round, String country) {
        List<String> tweets = Arrays.asList(tweetSentence.split(" "));
        for (String tweet : tweets) {
            if (!tweet.equals("") && tweet.length() > 2 && !tweet.equals("hiring") && !tweet.equals("careerarc")) {
                sendToNextTaskNum(country, tweet, round);
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
//            while (roundlist.get(0)<4068072)
//                roundlist.remove(0);
//            while (roundlist.get(roundlist.size()-1)>4069777)
//                roundlist.remove(roundlist.size()-1);

            int i = 0;
            while(2>i++)
                roundlist.remove(0);

            System.out.println(roundlist);

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
        System.out.println("cass: " + componentId);
        this.startRound = roundlist.get(0);
        ExcelWriter.putStartDate(new Date(), fileNum, this.startRound);
    }

    /**
     * Declare the output field "word"
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "round", "blockEnd"));
    }

}