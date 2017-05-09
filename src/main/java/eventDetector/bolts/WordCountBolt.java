package eventDetector.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cassandraConnector.CassandraDao;
import eventDetector.drawing.ExcelWriter;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.util.*;

public class WordCountBolt extends BaseRichBolt {

  private OutputCollector collector;
  private HashMap<String, Long> countsForRounds = null;
  private long currentRound = 0;
  private int threshold;
  private long ignoredCount = 0;
  private int componentId;
  private String fileNum;
  private String country;
  private Date lastDate = new Date();
  private Date startDate = new Date();
  private CassandraDao cassandraDao;
  private int numDetector;
  private int firstDetectorId;
  private int detectorTask;


  public WordCountBolt(int threshold, String filenum, String country, CassandraDao cassandraDao, int numDetector, int firstDetectorNum)
  {
    this.threshold = threshold;
    this.fileNum = filenum + "/";
    this.country = country;
    this.cassandraDao = cassandraDao;
    this.numDetector = numDetector;
    this.firstDetectorId = firstDetectorNum;
    this.detectorTask = firstDetectorNum;
  }
  @Override
  public void prepare(Map config, TopologyContext context,
                      OutputCollector collector) {
    this.collector = collector;
    this.countsForRounds = new HashMap<>();
    this.componentId = context.getThisTaskId()-1;
    System.out.println("wc : " + componentId );
  }

  @Override
  public void execute(Tuple tuple) {
    String word = tuple.getStringByField("word");
    long round = tuple.getLongByField("round");
    boolean blockend = tuple.getBooleanByField("blockEnd");

    if("dummyBLOCKdone".equals(word))
      for(int i=firstDetectorId;i<firstDetectorId+numDetector;i++)
        this.collector.emitDirect(i, new Values(word, round, false));

    TopologyHelper.writeToFile(Constants.WORKHISTORY_FILE + fileNum+ "workhistory.txt", new Date() + " Word count " + componentId + " working "  + round);
    if(blockend)
    {
      try {
        List<Object> values = new ArrayList<>();
        values.add(round);
        values.add(componentId);
        values.add(true);
        cassandraDao.insertIntoProcessed(values.toArray());

        for(int i=firstDetectorId;i<firstDetectorId+numDetector;i++)
          this.collector.emitDirect(i, new Values("BLOCKEND", round, true));
      } catch (Exception e) {
        e.printStackTrace();
      }

      TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
              "Word count "+ componentId + " end for round " + currentRound + " at " + lastDate);

      double diff = (lastDate.getTime()-startDate.getTime())/1000;
      if(diff==0.0) diff=1.0;
      TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
              "Word count "+ componentId + " time taken for round" + currentRound + " is " + diff );
      if ( currentRound!=0)
        ExcelWriter.putData(componentId,startDate,lastDate, "wc", country, currentRound);

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
      this.collector.emitDirect(detectorTask++, new Values(word, round, false));
      if(detectorTask==firstDetectorId+numDetector)
        detectorTask=firstDetectorId;
    }
      lastDate = new Date();

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("key", "round", "blockEnd"));
  }

}