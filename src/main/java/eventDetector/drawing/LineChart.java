package eventDetector.drawing;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import topologyBuilder.Constants;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class LineChart
{
  public static void drawLineChart(ArrayList<Long> tfidfs, String word, long round,
                                   String country, String drawFilePath ) throws IOException {
    DefaultCategoryDataset line_chart_dataset = new DefaultCategoryDataset();
    for(int i=0; i<tfidfs.size();i++)
      line_chart_dataset.addValue((Number) (tfidfs.get(i)), "tfidfs", i);

    JFreeChart lineChartObject = ChartFactory.createLineChart(
            "Tf-idf graph for \"" + word + "\" at round " + round + " in " + country,
            "Document order",
            "tf-idf Value",
            line_chart_dataset,PlotOrientation.VERTICAL,
            true,true,false);

    int width = 640; /* Width of the image */
    int height = 480; /* Height of the image */
    File lineChart = new File(drawFilePath + word + "-" + country + "-" + round +".jpeg" );
    ChartUtilities.saveChartAsJPEG(lineChart ,lineChartObject, width ,height);
  }
  public static void main( String[ ] args )
  {
    ArrayList<Long> vals_test = new ArrayList<>();
    vals_test.add(0L);
    vals_test.add(2L);
    vals_test.add(5L);
    vals_test.add(3L);
    vals_test.add(0L);
    vals_test.add(0L);
    vals_test.add(4L);
    vals_test.add(7L);
    vals_test.add(5L);
    vals_test.add(1L);
    vals_test.add(5L);
    vals_test.add(0L);
    vals_test.add(0L);
    vals_test.add(1L);
    vals_test.add(0L);

    try {
      drawLineChart(vals_test,"test",5,"USA", Constants.IMAGES_FILE_PATH);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}