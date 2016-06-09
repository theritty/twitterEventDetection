package eventDetector.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class WordCountBolt extends BaseRichBolt {

  private OutputCollector collector;
  private HashMap<Long, HashMap<String, Long>> countsWithRoundsUSA = null;
  private HashMap<Long, HashMap<String, Long>> countsWithRoundsCAN = null;
  private int threshold;

  public WordCountBolt(int threshold)
  {
    this.threshold = threshold;
  }
  @Override
  public void prepare(Map config, TopologyContext context,
                      OutputCollector collector) {
    this.collector = collector;
    this.countsWithRoundsUSA = new HashMap<>();
    this.countsWithRoundsCAN = new HashMap<>();
  }

  @Override
  public void execute(Tuple tuple) {
    String inputBolt = tuple.getStringByField( "inputBolt" );
    String source = (String) tuple.getValueByField( "source" );
    String country = (String) tuple.getValueByField( "country" );
    String word = tuple.getStringByField("word");
    long round = tuple.getLongByField("round");
    Boolean blockEnd = (Boolean) tuple.getValueByField("blockEnd");

    HashMap<Long, HashMap<String, Long>> countsWithRoundsTmp;
    if(country.equals("USA"))
      countsWithRoundsTmp = countsWithRoundsUSA;
    else
      countsWithRoundsTmp = countsWithRoundsCAN;

    if(blockEnd || word.equals("BLOCKEND"))
    {
      this.collector.emit(new Values("BLOCKEND", 1L, inputBolt, round, source, true, tuple.getValueByField("dates"), country));
      countsWithRoundsUSA.remove(round);
      countsWithRoundsCAN.remove(round);
      return;
    }
    else {
      countsWithRoundsTmp.putIfAbsent(round, new HashMap<>());
      Long count = countsWithRoundsTmp.get(round).get(word);
      if (count == null) {
        count = 0L;
      }
      count++;

      if (count > threshold) {
//        if(country.equals("CAN")) System.out.println("Counting: " + word + " " + count);
//        System.out.println("Word Count: " + word + " " + count);
        this.collector.emit(new Values(word, count, inputBolt, round, source, false, tuple.getValueByField("dates"), country));
      }

      countsWithRoundsTmp.get(round).put(word, count);
    }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "count", "inputBolt", "round", "source", "blockEnd", "dates", "country"));
  }

}