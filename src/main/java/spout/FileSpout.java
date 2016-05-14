package spout;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import topologies.topologyBuild.Constants;

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
        String str;
        for(Date fileName: fileNames)
        {
            try {
                currentDate = fileName;
                if(currentDate!=null) dates.add(currentDate);
                if(dates.size() > compareSize) dates.remove(0);

                String currentFileToRead = Constants.INPUT_FILE_PATH + inputFileNum + "/" + fileName.toString() + ".txt";
                this.fileReader = new FileReader(currentFileToRead);
                BufferedReader reader = new BufferedReader(fileReader);
                while((str = reader.readLine()) != null){
                    /**
                     * By each line emmit a new value with the line as a their
                     */
//                    Thread.sleep(5);
                    collector.emit(new Values(str, dates, currentDate, false, round, "file", "fileSpout"));

                }
//                System.out.println("Spout::: current " + currentDate + " dates " + dates);
                if(dates.size() > trainSize) {
//                    System.out.println("Block End " + currentDate + " Dates: ");
//                    for(Date d : dates)
//                        System.out.println("\t " + d);

                    collector.emit(new Values("BLOCKEND",
                            dates, currentDate, true, round++, "file", "fileSpout"));
                }
                else
                {
//                    System.out.println("First three " + currentDate + " Dates: " );
//                    for(Date d : dates)
//                        System.out.println("\t " + d);
                    collector.emit(new Values("BLOCKEND",
                            dates, currentDate, false, round++, "file", "fileSpout"));
                }

            } catch (FileNotFoundException e) {
                throw new RuntimeException("Error reading file ["+fileName.toString() + ".txt"+"]");
            } catch (IOException e) {
                throw new RuntimeException("Error reading tuple",e);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
            } finally{
               System.out.println("Reading " + fileName + " finished.");
            }

        }
        readed = true;

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
                //Wed May 04 02:13:29 EEST 2016

                try {
                    String dateString = listOfFiles[i].getName().replace(".txt", "");
                    DateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH);
                    Date date = format.parse(dateString);
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
        declarer.declare(new Fields("tweet", "dates","currentDate","blockEnd", "round", "source", "inputBolt"));
    }

}