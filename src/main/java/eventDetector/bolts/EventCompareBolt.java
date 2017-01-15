package eventDetector.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cassandraConnector.CassandraDao;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import eventDetector.drawing.ExcelWriter;
import eventDetector.drawing.LineChart;
import org.jfree.data.category.DefaultCategoryDataset;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class EventCompareBolt extends BaseRichBolt {
    private String drawFilePath;
    private CassandraDao cassandraDao;
    private int componentId;
    private String fileNum;
    private long currentRound = 0;
    private Date lastDate = new Date();
    private Date startDate = new Date();

    HashMap<Long, ArrayList<HashMap<String, Object>>> wordList;

    public EventCompareBolt(CassandraDao cassandraDao, String fileNum)
    {
        this.drawFilePath = Constants.IMAGES_FILE_PATH + fileNum +"/";
        wordList = new HashMap<>();
        this.cassandraDao = cassandraDao;
        this.fileNum = fileNum +"/";
    }

    @Override
    public void prepare(Map config, TopologyContext context,
                        OutputCollector collector) {
        this.componentId = context.getThisTaskId()-1;
        System.out.println("compare: " + componentId );
    }

    @Override
    public void execute(Tuple tuple) {

        ArrayList<Double> tfidfs = (ArrayList<Double>) tuple.getValueByField("tfidfs");
        String key = tuple.getStringByField("key");
        long round = tuple.getLongByField("round");
        String country = tuple.getStringByField("country");

        if("dummyBLOCKdone".equals(key)) {
            try {
                ExcelWriter.createTimeChart();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        TopologyHelper.writeToFile(Constants.WORKHISTORY_FILE, new Date() + " Compare " + componentId + " working " + round);
        if(currentRound < round) {

            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
                    "Compare bolt " + componentId + " end of round " + currentRound + " at " + lastDate);

            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + currentRound + ".txt",
                    "Word count "+ componentId + " time taken for round" + currentRound + " is " +
                            (lastDate.getTime()-startDate.getTime())/1000);
            if ( currentRound!=0)
                ExcelWriter.putData(componentId,startDate,lastDate, "Compare", "both", currentRound);
            startDate = new Date();
            TopologyHelper.writeToFile(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + round + ".txt",
                    "Compare bolt " + componentId + " start of round " + round + " at " + startDate);
            currentRound = round;
        }
        lastDate = new Date();


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
