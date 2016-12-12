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
    private Iterator<Row> iterator = null;
    private long current_round;
    private long count_tweets = 0;
    private String fileNum;
    private boolean start = true;


    public CassandraSpout(CassandraDao cassandraDao, int trainSize, int compareSize, int testSize, String filenum) throws Exception {
        this.cassandraDao = cassandraDao;
        this.compareSize = compareSize;
        this.trainSize = trainSize;
        this.testSize = testSize;
        roundlist = new ArrayList<>();
        readRoundlist = new ArrayList<>();
        this.fileNum = filenum + "/";
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
                try {
//          getEventInfo.report();
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

            try {
                if(!start) {
                    TopologyHelper.writeToFile("/Users/ozlemcerensahin/Desktop/workhistory.txt", new Date() + " Cass sleeping " + current_round);
                    Thread.sleep(300000);
                    TopologyHelper.writeToFile("/Users/ozlemcerensahin/Desktop/workhistory.txt", new Date() + " Cass wake up " + current_round);
                }
                else start = false;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

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
            collector.emit("USA", new Values("BLOCKEND", current_round, true, tmp_roundlist));
            collector.emit("CAN", new Values("BLOCKEND", current_round, true, tmp_roundlist));
            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + current_round + ".txt",
                    new Date() + ": Round end from cass spout =>" + current_round );

        }
    }

    public void splitAndEmit(String tweetSentence, long round, ArrayList<Long> roundlist, String country) {
        List<String> tweets = Arrays.asList(tweetSentence.split(" "));
        for (String tweet : tweets) {
            if (!tweet.equals("") && tweet.length() > 2 && !tweet.equals("hiring") && !tweet.equals("careerarc")) {
                this.collector.emit(country, new Values(tweet, round, false, roundlist));
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

            if(testSize!=Integer.MAX_VALUE) {
                while (roundlist.size() > testSize + trainSize)
                    roundlist.remove(roundlist.size() - 1);
            }
            int i = 0;
            while(trainSize>i++)
                readRoundlist.add(roundlist.remove(0));

            while (roundlist.get(0) < 2034735)
                roundlist.remove(0);

            int j = roundlist.size()-1;

//            while(roundlist.get(j)>2035083)
            while(roundlist.get(j)>2034855)
                roundlist.remove(j--);

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
    }

    /**
     * Declare the output field "word"
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("USA", new Fields("word", "round", "blockEnd", "dates"));
        declarer.declareStream("CAN", new Fields("word", "round", "blockEnd", "dates"));
    }

}