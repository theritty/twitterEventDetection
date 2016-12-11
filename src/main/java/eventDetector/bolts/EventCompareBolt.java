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
import org.jfree.data.category.DefaultCategoryDataset;
import topologyBuilder.Constants;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class EventCompareBolt extends BaseRichBolt {
  private String drawFilePath;
  private CassandraDao cassandraDao;
  private String componentId;

  HashMap<Long, ArrayList<HashMap<String, Object>>> wordList;

  public EventCompareBolt(CassandraDao cassandraDao, String fileNum)
  {
    this.drawFilePath = Constants.IMAGES_FILE_PATH + fileNum +"/";
    wordList = new HashMap<>();
    this.cassandraDao = cassandraDao;
  }

  @Override
  public void prepare(Map config, TopologyContext context,
                      OutputCollector collector) {
    this.componentId = String.valueOf(UUID.randomUUID());
  }

  @Override
  public void execute(Tuple tuple) {

    ArrayList<Double> tfidfs = (ArrayList<Double>) tuple.getValueByField("tfidfs");
    String key = tuple.getStringByField("key");
    long round = tuple.getLongByField("round");
    String country = tuple.getStringByField("country");

    System.out.println("Compare bolt " + componentId + " start of round " + round + " at " + new Date());
    if(tfidfs.size()<2) return;

    System.out.println(new Date() + ": Event found => " + key + " at round " + round  + " for " + country + " ");
    if(tfidfs.get(tfidfs.size()-2)==0) tfidfs.set(tfidfs.size()-2, 0.0001);
    try {
      cassandraDao.insertIntoEvents(round, country, key, tfidfs.get(tfidfs.size()-1) / tfidfs.get(tfidfs.size()-2));

      DefaultCategoryDataset countsList = getCountListFromCass(round, key, country);
      LineChart.drawLineChart(countsList,key,round,country, drawFilePath);
    } catch (Exception e) {
      e.printStackTrace();
    }

    System.out.println("Compare bolt " + componentId + " end of round " + round + " at " + new Date());
  }

  protected DefaultCategoryDataset getCountListFromCass(long round, String key, String country) throws Exception {
    long roundPast = round-10;

    DefaultCategoryDataset countsList = new DefaultCategoryDataset( );
    DateFormat df = new SimpleDateFormat("dd.MM.yyyy HH:mm");
    while(roundPast<=round) {
      ResultSet resultSet = cassandraDao.getFromCounts(roundPast, key, country);
      Iterator<Row> iterator = resultSet.iterator();
      if (iterator.hasNext()) {
        Row row = iterator.next();
        countsList.addValue(row.getLong("count"), "counts", df.format(new Date(new Long(roundPast) * 12*60*1000)));
      }
      else
        countsList.addValue(0L, "counts", df.format(new Date(new Long(roundPast) * 12*60*1000)));
      roundPast++;
    }
    return countsList;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer)
  {
  }
}
