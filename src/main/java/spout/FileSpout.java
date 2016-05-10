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

    public FileSpout(int trainSize, int compareSize)
    {
        fileNames = new ArrayList<>();
        round = 0;
        this.trainSize = trainSize;
        this.compareSize = compareSize;
    }
    public void ack(Object msgId) {}
    public void close() {}


    public void fail(Object msgId) {}

    /**
     * The only thing that the methods will do It is emit each
     * file line
     */
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
                if(currentDate!=null) dates.add(currentDate);
                if(dates.size() > trainSize) dates.remove(0);
                currentDate = fileName;

                String currentFileToRead = "tweets/" + fileName.toString() + ".txt";
                this.fileReader = new FileReader(currentFileToRead);
                BufferedReader reader = new BufferedReader(fileReader);
                while((str = reader.readLine()) != null){
                    /**
                     * By each line emmit a new value with the line as a their
                     */
//                    Thread.sleep(5);
                    collector.emit(new Values(str, dates, currentDate, false, round));

                }
//                System.out.println("Spout::: current " + currentDate + " dates " + dates);
                if(dates.size() > compareSize) {
                    System.out.println("Block End " + currentDate + " Dates: ");
                    for(Date d : dates)
                        System.out.println("\t " + d);

                    collector.emit(new Values("XXXXXXXXXXXXX----------END----------XXXXXXXXXXXXX",
                            dates, currentDate, true, round++));
                }
                else
                {
                    System.out.println("First three " + currentDate + " Dates: " );
                    for(Date d : dates)
                        System.out.println("\t " + d);
                    collector.emit(new Values("XXXXXXXXXXXXX----------END----------XXXXXXXXXXXXX",
                            dates, currentDate, false, round));
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
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {

        File folder = new File("tweets");
        File[] listOfFiles = folder.listFiles();

        for (int i = 0; i < listOfFiles.length; i++) {
            if (listOfFiles[i].isFile()) {
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

        System.out.println(fileNames.toString());
        this.collector = collector;
    }

    /**
     * Declare the output field "word"
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet", "dates","currentDate","blockEnd", "round"));
    }

}