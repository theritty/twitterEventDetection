package eventDetector.bolts;

import cassandraConnector.CassandraDao;
import eventDetector.drawing.ExcelWriter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class WordCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private HashMap<String, Long> countsForRounds = null;
    private long currentRound = 0;
    private int threshold;
    private long ignoredCount = 0;
    private int componentId;
    private String fileNum;
    private Date lastDate = new Date();
    private Date startDate = new Date();
    private String country;
    private CassandraDao cassandraDao;


    public WordCountBolt(int threshold, String filenum, String country, CassandraDao cassandraDao)
    {
        this.threshold = threshold;
        this.fileNum = filenum + "/";
        this.country = country;
        this.cassandraDao = cassandraDao;
    }
    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.countsForRounds = new HashMap<>();
        this.componentId = context.getThisTaskId()-1;
        System.out.println("wc : " + componentId  + " " + country);
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        long round = tuple.getLongByField("round");


        Date nowDate = new Date();
        if("dummyBLOCKdone".equals(word))
            this.collector.emit(new Values(word, round, false, tuple.getValueByField("dates"), tuple.getSourceStreamId()));

        TopologyHelper.writeToFile(Constants.WORKHISTORY_FILE + fileNum+ "workhistory.txt", new Date() + " Word count " + componentId + " working "  + round);
        if(round > currentRound)
        {
            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
                    "Word count "+ componentId + " end for round " + currentRound + " at " + lastDate);

            double diff = (lastDate.getTime()-startDate.getTime())/1000;
            if(diff==0.0) diff=1.0;
            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
                    "Word count "+ componentId + " time taken for round" + currentRound + " is " + diff );

            startDate = new Date();
            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + round + ".txt",
                    "Word count "+ componentId + " starting for round " + round + " at " + startDate );

            countsForRounds.clear();
            currentRound = round;
        }
        else if(round < currentRound) {
            ignoredCount++;
            if(ignoredCount%1000==0)
                TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + "ignoreCount.txt",
                        "Word count ignored count " + componentId + ": " + ignoredCount );
            return;
        }
        Long count = countsForRounds.get(word);

        if (count == null) count = 1L;
        else count++;

        countsForRounds.put(word, count);

        if (count == threshold) {
            this.collector.emit(new Values(word, round, false, tuple.getValueByField("dates"), tuple.getSourceStreamId()));
        }
        lastDate = new Date();

        ExcelWriter.putData(componentId,nowDate,lastDate, "wc",tuple.getSourceStreamId(), currentRound, cassandraDao);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "round", "blockEnd", "rounds", "country"));
    }

}