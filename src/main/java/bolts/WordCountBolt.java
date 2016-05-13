package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.*;

public class WordCountBolt extends BaseRichBolt {

    private OutputCollector collector;
    private HashMap<String, Long> counts = null;
    private long round;

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.counts = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String inputBolt = tuple.getStringByField( "inputBolt" );
        String source = (String) tuple.getValueByField( "source" );
        String word = tuple.getStringByField("word");
        long round = tuple.getLongByField("round");

        Long count = this.counts.get(word);
        if(count == null){
            count = 0L;
        }
        this.collector.emit(new Values(word, count, inputBolt, round,source));

        if(this.round < round)
        {
            this.counts.clear();
            this.round = round;
        }

        count++;
        this.counts.put(word, count);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count", "inputBolt", "round", "source"));
    }

}