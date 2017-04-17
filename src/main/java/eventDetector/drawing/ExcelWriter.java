package eventDetector.drawing;

import cassandraConnector.CassandraDao;
import com.datastax.driver.core.ResultSet;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import topologyBuilder.Constants;
import topologyBuilder.TopologyHelper;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * A very simple program that writes some data to an Excel file
 * using the Apache POI library.
 * @author www.codejava.net
 *
 */
public class ExcelWriter {

    private static int[][] times;
    private static Date startTime = new Date();
    private static long startRound = 0;
    private static String fileNum="12345";
    private static int lastInd ;
    private static int rowNum = 864000;
    private static int columnNum = 250;
    private static int parallelismNum = 20;
    //    private static int parallelismNum = 11;
    private static int writeChart = 0;


    public static void putStartDate(Date date, String filenum, long round) {
//        times = new int[rowNum][columnNum];
//        lastInd = rowNum -1;
        startTime = date;
        startRound = round;
//        for(int i = 0; i< rowNum; i++) {
//            times[i][0] = i;
//            for (int j = 1; j < columnNum; j++)
//                times[i][j] = 0;
//        }

        fileNum = filenum +"/";
    }

    public static void putData(int id, Date boltStartTime, Date boltEndTime, String boltName, String country, long round, CassandraDao cassandraDao) {

        long timeStart = (boltStartTime.getTime()-startTime.getTime())/1000;
        long duration = (boltEndTime.getTime()-boltStartTime.getTime())/1000;

        if(duration==0) duration = 1;

        while (duration-->0) {
            List<Object> values = new ArrayList<>();
            values.add((int) timeStart++);
            values.add(id + parallelismNum * ((int) ((round - startRound)) % 10));
            values.add(id);
            try {
                cassandraDao.insertIntoProcessTimes(values.toArray());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
//            times[(int) timeStart++][id + parallelismNum * ((int) ((round - startRound)/2) % 10) ] = id;
    }

    public static void cassandraTableToList(CassandraDao cassandraDao) {

        lastInd = rowNum -1;
        times = new int[rowNum][columnNum];

        for(int i = 0; i< rowNum; i++) {
            times[i][0] = i;
            for (int j = 1; j < columnNum; j++)
                times[i][j] = 0;
        }


        try {
            ResultSet resultSet;
            resultSet = cassandraDao.getProcessTimes();
            Iterator<com.datastax.driver.core.Row> iterator = resultSet.iterator();
            while (iterator.hasNext()) {
                com.datastax.driver.core.Row row = iterator.next();
                int process_row = row.getInt("row");
                int process_col = row.getInt("column");
                int process_id = row.getInt("id");

                //!!!!!!!!!!!!!!!!!!!!!!!!!!!
                times[process_row][process_col-1]=process_id;
                //!!!!!!!!!!!!!!!!!!!!!!!!!!!
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public static void createTimeChart (CassandraDao cassandraDao) throws IOException {
        writeChart++;
        if(writeChart>= 2) {
            TopologyHelper.writeToFile(Constants.RESULT_FILE_PATH + fileNum + "sout.txt", new Date() + " Excel creation started");
            System.out.println("Cass to table");
            cassandraTableToList(cassandraDao);
            System.out.println("writeToExcel");
            writeExcel();
            System.out.println("DONE");
        }
    }


    public static void clean () {
        for(int i = rowNum -1; i>=0; i--) {
            for(int j = 1; j<columnNum; j++) {
                if(times[i][j] != 0) {
                    lastInd = i+1;
                    return;
                }
            }
        }
    }

    public static void writeExcel() throws IOException {
        SXSSFWorkbook workbook = new SXSSFWorkbook();
        Sheet sheet = workbook.createSheet();
        clean();

        for (int i=0;i<lastInd;i++) {
            Row row = sheet.createRow(i);
            for (int j = 0; j<columnNum; j++) {
//                if(columnNum%100==0) System.out.println(i + " " + j);
                Cell cell = row.createCell(j);
//                if(times[i][j] == 0)
//                    cell.setCellValue("");
//                else
                cell.setCellValue(times[i][j]);
            }
        }

        try (FileOutputStream outputStream = new FileOutputStream(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + "timechart.xlsx")) {
            workbook.write(outputStream);
        }
        workbook.close();
    }

    public static void main(String[] args) throws Exception {
//        times = new int[rowNum][columnNum];
//        times[0][1] = 3;
//        times[9][7] = 3;
////        times[rowNum/8][5] = 5;
////        times[rowNum/4][136] = 36;
////        times[rowNum/2][190] = 10;
////        times[rowNum-10][12] = 11;
//        writeExcel();
        TopologyHelper topologyHelper = new TopologyHelper();
        Properties properties = topologyHelper.loadProperties( "config.properties" );


        int COUNT_THRESHOLD = Integer.parseInt(properties.getProperty("topology.count.threshold"));
        String FILENUM = properties.getProperty("topology.file.number");
        String TWEETS_TABLE = properties.getProperty("tweets.table");
        String COUNTS_TABLE = properties.getProperty("counts.table");
        String EVENTS_TABLE = properties.getProperty("events.table");
        String PROCESSTIMES_TABLE = properties.getProperty("processtimes.table");


        System.out.println("Count threshold " + COUNT_THRESHOLD);
        TopologyHelper.createFolder(Constants.RESULT_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.IMAGES_FILE_PATH + FILENUM);
        TopologyHelper.createFolder(Constants.TIMEBREAKDOWN_FILE_PATH + FILENUM);

        CassandraDao cassandraDao = new CassandraDao(TWEETS_TABLE, COUNTS_TABLE, EVENTS_TABLE, PROCESSTIMES_TABLE);
        writeChart=1;
        fileNum="testwordbased";
        createTimeChart(cassandraDao);
    }

}