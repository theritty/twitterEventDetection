package eventDetector.drawing;

import java.io.IOException;

import java.io.FileOutputStream;
import java.util.Date;
import java.util.HashMap;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import topologyBuilder.Constants;

/**
 * A very simple program that writes some data to an Excel file
 * using the Apache POI library.
 * @author www.codejava.net
 *
 */
public class ExcelWriter {

    private static int[][] times = new int[36000][20];
    private static Date startTime = new Date();
    private static HashMap<String,Integer> boltMapping = new HashMap<>();
    private static String fileNum="12345";

    public static void putStartDate(Date date, String filenum) {
        startTime = date;
        for(int i=0;i<36000;i++) {
            times[i][0] = i;
            for (int j = 1; j < 20; j++)
                times[i][j] = 0;
        }
        fileNum = filenum +"/";
    }

    public static void putData(String id, Date boltStartTime, Date boltEndTime, String boltName, String country) {

        if(boltMapping.get(id) == null) {
            boltMapping.put(id, boltMapping.size()+1);
        }
        int ind = boltMapping.get(id);

        long timeStart = (boltStartTime.getTime()-startTime.getTime())/1000;
        long duration = (boltEndTime.getTime()-boltStartTime.getTime())/1000;

        while (duration-->0)
            times[(int) timeStart++][ind] = ind;

        for(String entry: boltMapping.keySet()) {
            System.out.println( "Index: " + boltMapping.get(entry) +  ", key: " + entry + ", name: " + boltName + ", country: " + country );
        }
    }

    public static void createTimeChart () throws IOException {
        writeExcel(times);
    }


    public static void writeExcel(int[][] data) throws IOException {
        XSSFWorkbook workbook = new XSSFWorkbook();
        XSSFSheet sheet = workbook.createSheet("Java Books");

        int rowCount = 0;

        for (int[] aBook : data) {
            Row row = sheet.createRow(++rowCount);
            int columnCount = 0;
            for (Object field : aBook) {
                Cell cell = row.createCell(++columnCount);
                if (field instanceof String) {
                    cell.setCellValue((String) field);
                } else if (field instanceof Integer) {
                    cell.setCellValue((Integer) field);
                }
            }

        }
        try (FileOutputStream outputStream = new FileOutputStream(Constants.TIMEBREAKDOWN_FILE_PATH + fileNum + "timechart.xlsx")) {
            workbook.write(outputStream);
        }
        workbook.close();
    }

    public static void main(String[] args) throws IOException {
        int[][] bookData = {
                {2, 1, 79},
                {3, 2, 36},
                {4, 3, 42},
                {5, 4, 35},
        };
        writeExcel(bookData);
    }

}