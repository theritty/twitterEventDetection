package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.*;

public class EventCompareBolt extends BaseRichBolt {

    private OutputCollector collector;
    private int fileNum;
    private long currentRound = 0;
    ArrayList<HashMap<String, Object>> wordList;

    public EventCompareBolt(int fileNum)
    {
        this.fileNum = fileNum;
        wordList = new ArrayList<>();
    }

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {

        ArrayList<Double> tfidfs = (ArrayList<Double>) tuple.getValueByField("tfidfs");
        String key = tuple.getStringByField("key");
        String type = tuple.getStringByField("type");
        long round = tuple.getLongByField("round");

        ArrayList<ArrayList<HashMap<String, Object>>> compareList = new ArrayList<>();

        if (round > currentRound ) {
            for (HashMap<String, Object> hm : wordList) {
                String currentKey = (String) hm.get("word");
                ArrayList<Double> currentTfidfs = (ArrayList<Double>) hm.get("tfidfs");
                boolean added = false;


                for (ArrayList<HashMap<String, Object>> al : compareList) {
                    for (HashMap<String, Object> chm : al) {
                        String keyToCompare = (String) chm.get("word");
                        ArrayList<Double> tfidfToCompare = (ArrayList<Double>) chm.get("tfidfs");
                        int cnt = 0;

                        for (int i = 0; i < tfidfToCompare.size(); i++) {
                            if (currentTfidfs.get(i) != 0 && tfidfToCompare.get(i) != 0) cnt++;
                        }

                        if ( ( (double) cnt / (double) tfidfToCompare.size()) > 0.5) {
                            added = true;
                            al.add(hm);
                            break;
                        }
                    }
                    if (added) break;
                }


                if (!added) {
                    ArrayList<HashMap<String, Object>> tmp = new ArrayList<>();
                    tmp.add(hm);
                    compareList.add(tmp);
                }
            }
            if(compareList.size()>0)
                writeToFile(fileNum + "/events-" + round, compareList);
            wordList.clear();
            currentRound = round;
        }
        HashMap<String, Object> xx = new HashMap<>();
        xx.put("word", key);
        xx.put("type", type);
        xx.put("tfidfs", tfidfs);

        wordList.add(xx);
    }

    public void writeToFile(String fileName,  ArrayList<ArrayList<HashMap<String, Object>>> compareList)
    {
        try {
            PrintWriter writer = new PrintWriter(new FileOutputStream(
                    new File(fileName),
                    true /* append = true */));
            int cnt = 1;
            for (ArrayList<HashMap<String, Object>> al : compareList) {
                write(writer, "Event " + cnt);
                for (HashMap<String, Object> chm : al) {
                    if(chm.get("type").equals("hashtag"))
                    {
                        write(writer, "\t#" + chm.get("word") + " " + ((ArrayList<Double>) chm.get("tfidfs")).toString());
                    }
                    else
                    {
                        write(writer, "\t" + chm.get("word") + " " + ((ArrayList<Double>) chm.get("tfidfs")).toString());
                    }

                }
                cnt++;
            }

            writer.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void write(PrintWriter writer, String line) {
        writer.println(line);
//        System.out.println(line);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        //declarer.declare(new Fields("word"));
    }
}
