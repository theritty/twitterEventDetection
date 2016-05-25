package drawing;

import java.io.*;
import java.util.ArrayList;

import org.jfree.chart.JFreeChart;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;
import topologies.topologyBuild.Constants;

public class LineChart
{
  public static void drawLineChart(ArrayList<Double> tfidfs, String word, long round,
                                   String country, String drawFilePath ) throws IOException {
    DefaultCategoryDataset line_chart_dataset = new DefaultCategoryDataset();
    for(int i=0; i<tfidfs.size();i++)
      line_chart_dataset.addValue((Number) (tfidfs.get(i) * 1000), "tfidfs", i);

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
    ArrayList<Double> vals_test = new ArrayList<>();
    vals_test.add(0.0);
    vals_test.add(2.152566785074336E-4);
    vals_test.add(2.0602791956360036E-5);
    vals_test.add(3.158834297930437E-4);
    vals_test.add(0.0);
    vals_test.add(0.0);
    vals_test.add(4.1343986383876874E-5);
    vals_test.add(1.7418468643986954E-4);
    vals_test.add(0.0);
    vals_test.add(1.3877360058842453E-4);
    vals_test.add(5.770302249032202E-5);
    vals_test.add(0.0);
    vals_test.add(0.0);
    vals_test.add(1.5113743075290971E-4);
    vals_test.add(0.001896515850769457);

    try {
      drawLineChart(vals_test,"test",5,"USA",Constants.IMAGES_FILE_PATH);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}