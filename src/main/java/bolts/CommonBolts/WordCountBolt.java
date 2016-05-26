package bolts.CommonBolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import exampleWordCount.WordCount;

import java.util.*;

public class WordCountBolt extends BaseRichBolt {

  private OutputCollector collector;
  private HashMap<Long, HashMap<String, Long>> countsWithRounds = null;
  private long round;
  private int threshold;

  public WordCountBolt(int threshold)
  {
    this.threshold = threshold;
  }
  @Override
  public void prepare(Map config, TopologyContext context,
                      OutputCollector collector) {
    this.collector = collector;
    this.countsWithRounds = new HashMap<>();
  }

  @Override
  public void execute(Tuple tuple) {
    String inputBolt = tuple.getStringByField( "inputBolt" );
    String source = (String) tuple.getValueByField( "source" );
    String country = (String) tuple.getValueByField( "country" );
    String word = tuple.getStringByField("word");
    long round = tuple.getLongByField("round");
    Boolean blockEnd = (Boolean) tuple.getValueByField("blockEnd");

    if(blockEnd || word.equals("BLOCKEND"))
    {
      this.collector.emit(new Values("BLOCKEND", 1L, inputBolt, round, source, true, tuple.getValueByField("dates"), country));
      countsWithRounds.remove(round);
      return;
    }
    else {
      countsWithRounds.putIfAbsent(round, new HashMap<>());
      Long count = this.countsWithRounds.get(round).get(word);
      if (count == null) {
        count = 0L;
      }
      count++;

      if (count > threshold) {
//        System.out.println("Word count " + word + " " + count);
        this.collector.emit(new Values(word, count, inputBolt, round, source, false, tuple.getValueByField("dates"), country));
      }

      this.countsWithRounds.get(round).put(word, count);
    }

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "count", "inputBolt", "round", "source", "blockEnd", "dates", "country"));
  }

}