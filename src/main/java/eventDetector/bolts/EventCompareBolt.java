package eventDetector.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cassandraConnector.CassandraDao;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import eventDetector.drawing.LineChart;
import topologyBuilder.Constants;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class EventCompareBolt extends BaseRichBolt {

  private OutputCollector collector;
  private String filePath;
  private String drawFilePath;
  private double rateForSameEvent;
  private CassandraDao cassandraDao;

  HashMap<Long, ArrayList<HashMap<String, Object>>> wordList;

  public EventCompareBolt(CassandraDao cassandraDao, String filePath, int fileNum, double rateForSameEvent)
  {
    this.rateForSameEvent = rateForSameEvent;
    this.filePath = filePath + fileNum + "/";
    this.drawFilePath = Constants.IMAGES_FILE_PATH + fileNum +"/";
    wordList = new HashMap<>();
    this.cassandraDao = cassandraDao;
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
    long round = tuple.getLongByField("round");
    String country = tuple.getStringByField("country");


    if(tfidfs.get(tfidfs.size()-2)==0) tfidfs.set(tfidfs.size()-2, 0.0001);
    try {
      cassandraDao.insertIntoEvents(round, country, key, tfidfs.get(tfidfs.size()-1) / tfidfs.get(tfidfs.size()-2));

      ArrayList<Long> countsList = getCountListFromCass(round, key, country);
      LineChart.drawLineChart(countsList,key,round,country, drawFilePath);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  protected ArrayList<Long> getCountListFromCass(long round, String key, String country) throws Exception {
    long roundPast = round-10;
    ArrayList<Long> countsList = new ArrayList<>();
    while(roundPast<=round) {
      ResultSet resultSet = cassandraDao.getFromCounts(roundPast, key, country);
      Iterator<Row> iterator = resultSet.iterator();
      if (iterator.hasNext()) {
        Row row = iterator.next();
        countsList.add(row.getLong("count"));
      }
      else
        countsList.add(0L);
      roundPast++;
    }
    return countsList;
  }
//  public void writeToFile(String fileName,  ArrayList<ArrayList<HashMap<String, Object>>> compareList)
//  {
//    File filePath = new File(fileName);
//    filePath.delete();
//
//    try {
//      PrintWriter writer = new PrintWriter(new FileOutputStream(
//              new File(fileName),
//              true /* append = true */));
//      writer.print("");
//      int cnt = 1;
//      for (ArrayList<HashMap<String, Object>> al : compareList) {
//        write(writer, "Event " + cnt);
//        for (HashMap<String, Object> chm : al) {
//          if(chm.get("type").equals("hashtag"))
//          {
//            write(writer, "\t#" + chm.get("word") + " " + (chm.get("tfidfs")).toString());
//          }
//          else
//          {
//            write(writer, "\t" + chm.get("word") + " " + (chm.get("tfidfs")).toString());
//          }
//        }
//        cnt++;
//      }
//
//      writer.close();
//
//    } catch (FileNotFoundException e) {
//      e.printStackTrace();
//    }
//  }

//  public void write(PrintWriter writer, String line) {
//    writer.println(line);
////        System.out.println(line);
//  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
  }
}
