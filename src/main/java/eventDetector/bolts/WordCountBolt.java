package eventDetector.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import cassandraConnector.CassandraDao;
import com.datastax.driver.core.Row;
import eventDetector.drawing.ExcelWriter;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.util.*;

public class WordCountBolt extends BaseRichBolt {

  private OutputCollector collector;
  private HashMap<String, Long> countsForRounds = null;
  private long currentRound = 0;
  private int threshold;
  private long ignoredCount = 0;
  private int componentId;
  private String fileNum;
  private Date lastDate = new Date();
  private Date startDate = new Date();
    private String country;
  private CassandraDao cassandraDao;
  private HashMap<Long, Long> counts = new HashMap<>();

  private int USATask=13;
  private int CANTask=15;


  public WordCountBolt(int threshold, String filenum, String country, CassandraDao cassandraDao)
  {
    this.threshold = threshold;
    this.fileNum = filenum + "/";
      this.country = country;
    this.cassandraDao = cassandraDao;
  }
  @Override
  public void prepare(Map config, TopologyContext context,
                      OutputCollector collector) {
    this.collector = collector;
    this.countsForRounds = new HashMap<>();
    this.componentId = context.getThisTaskId()-1;
    System.out.println("wc : " + componentId  + " " + country);
  }

  @Override
  public void execute(Tuple tuple) {
    String word = tuple.getStringByField("word");
    long round = tuple.getLongByField("round");
    boolean blockEnd = tuple.getBooleanByField("blockEnd");


    USATask = USATask%2+13;
    CANTask = CANTask%2+15;
    Date nowDate = new Date();
    if(blockEnd) {
      System.out.println( new Date() + " round end " + round + " for " + country + " for " + componentId);

      try {
        Iterator<Row> iteratorProcessed = cassandraDao.getProcessed(round, componentId).iterator();
        List<Object> values = new ArrayList<>();
        values.add(round);
        values.add(componentId);
        values.add(iteratorProcessed.next().getLong("spoutSent"));
        values.add(counts.get(round));
        values.add(true);
        values.add(country);
        cassandraDao.insertIntoProcessed(values.toArray());

        Iterator<Row> iteratorByCountry = cassandraDao.getProcessedByCountry(round, country).iterator();
        while (iteratorByCountry.hasNext()){
          Row r = iteratorByCountry.next();
          if(r.getInt("boltId")<13 && !r.getBool("finished")) {
            System.out.println("I am " + componentId + ", " +  r.getInt("boltId") + " is not finished.");
            return;
          }
        }
          System.out.println("USA sending finish");
          this.collector.emitDirect(13, new Values(word, round, true, tuple.getValueByField("dates"), "USA"));
          this.collector.emitDirect(14, new Values(word, round, true, tuple.getValueByField("dates"), "USA"));
          System.out.println("CAN sending finish");
          this.collector.emitDirect(15, new Values(word, round, true, tuple.getValueByField("dates"), "CAN"));
          this.collector.emitDirect(16, new Values(word, round, true, tuple.getValueByField("dates"), "CAN"));

        counts.remove(round);

      } catch (Exception e) {
        e.printStackTrace();
      }
    }


    TopologyHelper.writeToFile(Constants.WORKHISTORY_FILE + fileNum+ "workhistory.txt", new Date() + " Word count " + componentId + " working "  + round);
    if(round > currentRound)
    {
      TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
              "Word count "+ componentId + " end for round " + currentRound + " at " + lastDate);

      double diff = (lastDate.getTime()-startDate.getTime())/1000;
      if(diff==0.0) diff=1.0;
      TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
              "Word count "+ componentId + " time taken for round" + currentRound + " is " + diff );

      startDate = new Date();
      TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + round + ".txt",
              "Word count "+ componentId + " starting for round " + round + " at " + startDate );

      countsForRounds.clear();
      currentRound = round;
    }
    else if(round < currentRound) {
      ignoredCount++;
      if(ignoredCount%1000==0)
        TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + "ignoreCount.txt",
              "Word count ignored count " + componentId + ": " + ignoredCount );
      return;
    }
    Long count = countsForRounds.get(word);

    if (count == null) count = 1L;
    else count++;

    countsForRounds.put(word, count);

    if (count == threshold) {
      if(country.equals("USA"))
        this.collector.emitDirect(USATask++, new Values(word, round, false, tuple.getValueByField("dates"), country));
      else
        this.collector.emitDirect(CANTask++, new Values(word, round, false, tuple.getValueByField("dates"), country));

    }
    lastDate = new Date();

    ExcelWriter.putData(componentId,nowDate,lastDate, "wc",tuple.getSourceStreamId(), currentRound);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("key", "round", "blockEnd", "rounds", "country"));
  }

}