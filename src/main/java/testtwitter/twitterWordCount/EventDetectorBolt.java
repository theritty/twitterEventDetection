package testtwitter.twitterWordCount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.*;

public class EventDetectorBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {

        ArrayList<String> dates = (ArrayList<String>)tuple.getValueByField("dates");
        String key = tuple.getStringByField("key");
        String type = tuple.getStringByField("type");
        int round = tuple.getIntegerByField("round");

        ArrayList<Double> tfidfs = new ArrayList<>();

        for (String date: dates)
        {
            TFIDFCalculator calculator = new TFIDFCalculator();
            tfidfs.add(calculator.tfIdf(dates,key,date));
        }

        System.out.println("Tfidfs:::::" + tfidfs.toString() + "for word " + key + " round " + round  );
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        //declarer.declare(new Fields("word"));
    }
}
