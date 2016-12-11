package eventDetector.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

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

  public WordCountBolt(int threshold)
  {
    this.threshold = threshold;
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

    if(round > currentRound)
    {
      System.out.println("Word count "+ componentId + " starting for round " + round + " at " + new Date());
      countsForRounds.clear();
      currentRound = round;
    }
    else if(round < currentRound) {
      ignoredCount++;
      if(ignoredCount%1000==0)
        System.out.println("Ignored count " + componentId + ": " + ignoredCount);
      return;
    }

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
    declarer.declare(new Fields("word", "round", "blockEnd", "dates", "country"));
  }

}