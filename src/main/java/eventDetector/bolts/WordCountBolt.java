package eventDetector.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import eventDetector.drawing.ExcelWriter;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class WordCountBolt extends BaseRichBolt {

  private OutputCollector collector;
  private HashMap<String, Long> countsForRounds = null;
  private long currentRound = 0;
  private int threshold;
  private long ignoredCount = 0;
  private String componentId;
  private String fileNum;
  private Date lastDate = new Date();
  private Date startDate = new Date();


  public WordCountBolt(int threshold, String filenum)
  {
    this.threshold = threshold;
    this.fileNum = filenum + "/";
  }
  @Override
  public void prepare(Map config, TopologyContext context,
                      OutputCollector collector) {
    this.collector = collector;
    this.countsForRounds = new HashMap<>();
    this.componentId = String.valueOf(UUID.randomUUID());
  }

  @Override
  public void execute(Tuple tuple) {
    String word = tuple.getStringByField("word");
    long round = tuple.getLongByField("round");

    if("dummyBLOCKdone".equals(word))
       this.collector.emit(new Values(word, round, false, tuple.getValueByField("dates"), tuple.getSourceStreamId()));

    TopologyHelper.writeToFile("/Users/ozlemcerensahin/Desktop/workhistory.txt", new Date() + " Word count " + componentId + " working "  + round);
    if(round > currentRound)
    {
      TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
              "Word count "+ componentId + " end for round " + currentRound + " at " + lastDate);

      TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
              "Word count "+ componentId + " time taken for round" + currentRound + " is " +
                      (lastDate.getTime()-startDate.getTime())/1000);
      if ( currentRound!=0)
        ExcelWriter.putData(componentId,startDate,lastDate, "wc",tuple.getSourceStreamId() );

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
    lastDate = new Date();
    Long count = countsForRounds.get(word);

    if (count == null) count = 1L;
    else count++;

    countsForRounds.put(word, count);

    if (count == threshold) {
      this.collector.emit(new Values(word, round, false, tuple.getValueByField("dates"), tuple.getSourceStreamId()));
    }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("key", "round", "blockEnd", "rounds", "country"));
  }

}