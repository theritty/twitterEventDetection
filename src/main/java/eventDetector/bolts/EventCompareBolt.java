package eventDetector.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import eventDetector.drawing.LineChart;
import topologyBuilder.Constants;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class EventCompareBolt extends BaseRichBolt {

  private OutputCollector collector;
  private String filePath;
  private String drawFilePath;
  private double rateForSameEvent;
  HashMap<Long, ArrayList<HashMap<String, Object>>> wordList;

  public EventCompareBolt(String filePath, int fileNum, double rateForSameEvent)
  {
    this.rateForSameEvent = rateForSameEvent;
    this.filePath = filePath + fileNum + "/";
    this.drawFilePath = Constants.IMAGES_FILE_PATH + fileNum +"/";
    wordList = new HashMap<>();
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
    String country = tuple.getStringByField("country");

    ArrayList<ArrayList<HashMap<String, Object>>> compareList = new ArrayList<>();
    RoundInfo roundInfo;
    if(wordList.get(round) == null)
    {
      wordList.put(round, new ArrayList<>());
    }
    HashMap<String, Object> xx = new HashMap<>();
    xx.put("word", key);
    xx.put("type", type);
    xx.put("tfidfs", tfidfs);

    wordList.get(round).add(xx);

//        if (round > currentRound ) {
//    for (HashMap<String, Object> hm : wordList.get(round)) {
//      String currentKey = (String) hm.get("word");
//      ArrayList<Double> currentTfidfs = (ArrayList<Double>) hm.get("tfidfs");
//      boolean added = false;
//
//
//      for (ArrayList<HashMap<String, Object>> al : compareList) {
//        for (HashMap<String, Object> chm : al) {
//          String keyToCompare = (String) chm.get("word");
//          ArrayList<Double> tfidfToCompare = (ArrayList<Double>) chm.get("tfidfs");
//          int cnt = 0;
//
//          for (int i = 0; i < tfidfToCompare.size(); i++) {
//            if (currentTfidfs.get(i) != 0 && tfidfToCompare.get(i) != 0) cnt++;
//          }
//
//          if ( ( (double) cnt / (double) tfidfToCompare.size()) > rateForSameEvent) {
//            added = true;
//            al.add(hm);
//            break;
//          }
//        }
//        if (added) break;
//      }
//      if (!added) {
//        ArrayList<HashMap<String, Object>> tmp = new ArrayList<>();
//        tmp.add(hm);
//        compareList.add(tmp);
//      }
//    }
    compareList.add(wordList.get(round));
    if(compareList.size()>0) {
      writeToFile(filePath + "events-" + country + "-" + round, compareList);
      try {
        LineChart.drawLineChart(tfidfs,key,round,country, drawFilePath);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

//    wordList.clear();
//    currentRound = round;
//        }

  }

  public void writeToFile(String fileName,  ArrayList<ArrayList<HashMap<String, Object>>> compareList)
  {
    File filePath = new File(fileName);
    filePath.delete();

    try {
      PrintWriter writer = new PrintWriter(new FileOutputStream(
              new File(fileName),
              true /* append = true */));
      writer.print("");
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
