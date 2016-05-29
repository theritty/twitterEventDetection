package trials.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import trials.topologies.topologyBuild.Constants;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class FileSpout extends BaseRichSpout {

  private SpoutOutputCollector collector;
  private FileReader fileReader;
  private boolean readed = false;
  private ArrayList<Date> fileNames;
  ArrayList<Date> dates = new ArrayList<>();
  Date currentDate = null;
  long round ;
  int trainSize;
  int compareSize;
  int inputFileNum;

  public FileSpout(int trainSize, int compareSize, int inputFileNum)
  {
    fileNames = new ArrayList<>();
    round = 0;
    this.trainSize = trainSize;
    this.compareSize = compareSize;
    this.inputFileNum = inputFileNum;
  }
  @Override
  public void ack(Object msgId) {}
  @Override
  public void close() {}

  @Override
  public void fail(Object msgId) {}

  /**
   * The only thing that the methods will do It is emit each
   * file line
   */
  @Override
  public void nextTuple() {
    /**
     * The nextuple it is called forever, so if we have been readed the file
     * we will wait and then return
     */
    if(readed){
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        //Do nothing
      }
      return;
    }

    for(Date fileName: fileNames)
    {
      try {
        currentDate = fileName;
        if(currentDate!=null) dates.add(currentDate);
        if(dates.size() > compareSize) dates.remove(0);

        if(dates.size() <= trainSize) continue;


        readData(fileName,"USA");
        readData(fileName,"CAN");

      } catch (FileNotFoundException e) {
        throw new RuntimeException("Error reading file ["+fileName.toString() + ".txt"+"]");
      } catch (IOException e) {
        throw new RuntimeException("Error reading tuple",e);
      }
    }
    readed = true;
    System.out.println("Reading finished.");

  }

  public void readData(Date fileName, String country) throws IOException {
    String str;
    String currentFileToRead = Constants.INPUT_FILE_PATH + inputFileNum + "/" + fileName.toString() +"-" + country + ".txt";
    this.fileReader = new FileReader(currentFileToRead);
    BufferedReader reader = new BufferedReader(fileReader);
    while((str = reader.readLine()) != null){
      /**
       * By each line emmit a new value with the line as a their
       */
      collector.emit(new Values(str, dates, currentDate, false, round, "file", "fileSpout", country));

    }
    if(dates.size() > trainSize) {
      ArrayList<Date> tmp_dates = new ArrayList<>(dates);
      collector.emit(new Values("BLOCKEND",
              tmp_dates, currentDate, true, round++, "file", "fileSpout", country));
    }
  }

  /**
   * We will create the file and get the collector object
   */
  @Override
  public void open(Map conf, TopologyContext context,
                   SpoutOutputCollector collector) {

    File folder = new File(Constants.INPUT_FILE_PATH + inputFileNum);
    File[] listOfFiles = folder.listFiles();

    for (int i = 0; i < listOfFiles.length; i++) {
      if (listOfFiles[i].isFile() && !listOfFiles[i].getName().contains("~") ) {
        try {
          String rawName = listOfFiles[i].getName().replace(".txt", "");
          String dateString = rawName.split("-")[0];
          DateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
          Date date = format.parse(dateString);
          if((rawName.split("-")[1]).equals("USA"))
            fileNames.add(date);
        } catch (ParseException e) {
          e.printStackTrace();
        }

      }
    }
    Collections.sort(fileNames, new Comparator<Date>() {
      public int compare(Date m1, Date m2) {
        return m1.compareTo(m2);
      }
    });

//        System.out.println(fileNames.toString());
    this.collector = collector;
  }

  /**
   * Declare the output field "word"
   */
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("tweet", "dates","currentDate","blockEnd", "round", "source", "inputBolt", "country"));
  }

}