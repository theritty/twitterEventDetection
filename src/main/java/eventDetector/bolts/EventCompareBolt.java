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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class EventCompareBolt extends BaseRichBolt {
  private String drawFilePath;
  private CassandraDao cassandraDao;

  HashMap<Long, ArrayList<HashMap<String, Object>>> wordList;

  public EventCompareBolt(CassandraDao cassandraDao, int fileNum, double rateForSameEvent)
  {
    this.drawFilePath = Constants.IMAGES_FILE_PATH + fileNum +"/";
    wordList = new HashMap<>();
    this.cassandraDao = cassandraDao;
  }

  @Override
  public void prepare(Map config, TopologyContext context,
                      OutputCollector collector) {
  }

  @Override
  public void execute(Tuple tuple) {

    ArrayList<Double> tfidfs = (ArrayList<Double>) tuple.getValueByField("tfidfs");
    String key = tuple.getStringByField("key");
    long round = tuple.getLongByField("round");
    String country = tuple.getStringByField("country");

    if(tfidfs.size()<2) return;
    System.out.println("Compare: in " + key);

    if(tfidfs.get(tfidfs.size()-2)==0) tfidfs.set(tfidfs.size()-2, 0.0001);
    try {
      cassandraDao.insertIntoEvents(round, country, key, tfidfs.get(tfidfs.size()-1) / tfidfs.get(tfidfs.size()-2));

      ArrayList<Long> countsList = getCountListFromCass(round, key, country);
      LineChart.drawLineChart(countsList,key,round,country, drawFilePath);
    } catch (Exception e) {
      e.printStackTrace();
    }


    System.out.println("Compare: out " + key);
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

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
  }
}
