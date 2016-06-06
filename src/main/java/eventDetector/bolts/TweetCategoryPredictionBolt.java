package eventDetector.bolts;

import java.io.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.GeoLocation;
import twitter4j.Status;
import twitter4j.User;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.meta.FilteredClassifier;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.Utils;
import weka.filters.unsupervised.attribute.StringToWordVector;

public class TweetCategoryPredictionBolt extends BaseRichBolt {
	private FilteredClassifier politicsClassifier;
	private FilteredClassifier musicClassifier;
	private FilteredClassifier sportsClassifier;

	static File resourcesDirectory = new File("src/main/resources");
	private static String politicsTrainingFileName = "politicsTweets.txt";
<<<<<<< HEAD
	private static String musicTrainingFileName = "musicTweets.txt";
	private static String sportsTrainingFileName = "sportsTweets.txt";

=======
	private static String musicTrainingFileName = "politicsTweets.txt";
	private static String sportsTrainingFileName = "politicsTweets.txt";
	
>>>>>>> 17235abe2b3218a98f8a94e7d6e0c4211e2aeaa1
	// Define attributes
	private Attribute tweetAttribute;
	private Attribute politicsAttribute;
	private Attribute musicAttribute;
	private Attribute sportsAttribute;

	private FastVector politicsFv;
	private FastVector musicFv;
	private FastVector sportsFv;

	private FastVector politicsClassifierFv;
	private FastVector musicClassifierFv;
	private FastVector sportsClassifierFv;

	private OutputCollector collector;

	public static BufferedReader readDataFile(String filename) {
		File dataFile = new File(resourcesDirectory.getAbsolutePath() + File.separator + filename);
		BufferedReader inputReader = null;

		try {
			inputReader = new BufferedReader(new FileReader(dataFile.getAbsolutePath()));
		} catch (FileNotFoundException ex) {
			System.err.println("File not found: " + filename);
		}

		return inputReader;
	}



	@Override
	public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
		// Read train data
		BufferedReader politicsTrainingFile = readDataFile(politicsTrainingFileName);
		BufferedReader musicTrainingFile = readDataFile(musicTrainingFileName);
		BufferedReader sportsTrainingFile = readDataFile(sportsTrainingFileName);

		Instances politicsTrainData = null;
		try {
			politicsTrainData = new Instances(politicsTrainingFile);

			politicsTrainData.setClassIndex(politicsTrainData.numAttributes() - 1);

			Instances musicTrainData = new Instances(musicTrainingFile);
			musicTrainData.setClassIndex(musicTrainData.numAttributes() - 1);

			Instances sportsTrainData = new Instances(sportsTrainingFile);
			sportsTrainData.setClassIndex(sportsTrainData.numAttributes() - 1);

			// To handle string attributes
			StringToWordVector filter = new StringToWordVector();
			filter.setAttributeNamePrefix("taylan");
			filter.setAttributeIndices("first-last");

			// Prepare classifiers
			politicsClassifier = new FilteredClassifier();
			politicsClassifier.setFilter(filter);
			politicsClassifier.setClassifier(new NaiveBayes());
			politicsClassifier.buildClassifier(politicsTrainData);

			musicClassifier = new FilteredClassifier();
			musicClassifier.setFilter(filter);
			musicClassifier.setClassifier(new NaiveBayes());
			musicClassifier.buildClassifier(musicTrainData);

			sportsClassifier = new FilteredClassifier();
			sportsClassifier.setFilter(filter);
			sportsClassifier.setClassifier(new NaiveBayes());
			sportsClassifier.buildClassifier(sportsTrainData);

			// Define attributes
			tweetAttribute = new Attribute("tweet", (FastVector) null);

			politicsFv = new FastVector(2);
			politicsFv.addElement("yes");
			politicsFv.addElement("no");
			politicsAttribute = new Attribute("politics", politicsFv);

			musicFv = new FastVector(2);
			musicFv.addElement("yes");
			musicFv.addElement("no");
			musicAttribute = new Attribute("music", musicFv);

			sportsFv = new FastVector(2);
			sportsFv.addElement("yes");
			sportsFv.addElement("no");
			sportsAttribute = new Attribute("sports", sportsFv);


			// Put attributes in a vector
			politicsClassifierFv = new FastVector(2);
			politicsClassifierFv.addElement(tweetAttribute);
			politicsClassifierFv.addElement(politicsAttribute);

			musicClassifierFv = new FastVector(2);
			musicClassifierFv.addElement(tweetAttribute);
			musicClassifierFv.addElement(musicAttribute);

			sportsClassifierFv = new FastVector(2);
			sportsClassifierFv.addElement(tweetAttribute);
			sportsClassifierFv.addElement(sportsAttribute);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}



	@Override
	public void execute(Tuple tuple) {
		// Create a dataset for each classifer, using the attributes
<<<<<<< HEAD
		List<String> tweet_words = (List<String>) tuple.getValueByField( "tweet" );
		String tweet = "";
		List<String> predictedCategories = new ArrayList<>();

		try {
			if(tweet_words== null || tweet_words.size()==0) return;
			for(String twee:tweet_words)
				tweet = tweet + twee + " ";

			Instances politicsData = new Instances("category", politicsClassifierFv, 0);
			politicsData.setClassIndex(politicsData.numAttributes() - 1);

			Instances musicData = new Instances("category", musicClassifierFv, 0);
			musicData.setClassIndex(musicData.numAttributes() - 1);

			Instances sportsData = new Instances("category", sportsClassifierFv, 0);
			sportsData.setClassIndex(sportsData.numAttributes() - 1);

			// Create an instance for each classifier, using the given arguments
			double[] politicsTestValues = new double[politicsData.numAttributes()];
			politicsTestValues[0] = politicsData.attribute(0).addStringValue(tweet);
			politicsTestValues[1] = Utils.missingValue();

			double[] musicTestValues = new double[musicData.numAttributes()];
			musicTestValues[0] = musicData.attribute(0).addStringValue(tweet);
			musicTestValues[1] = Utils.missingValue();

			double[] sportsTestValues = new double[sportsData.numAttributes()];
			sportsTestValues[0] = sportsData.attribute(0).addStringValue(tweet);
			sportsTestValues[1] = Utils.missingValue();
=======
		Instances politicsData = new Instances("category", politicsClassifierFv, 0);
		politicsData.setClassIndex(politicsData.numAttributes() - 1);
		
		Instances musicData = new Instances("category", musicClassifierFv, 0);
		musicData.setClassIndex(musicData.numAttributes() - 1);
		
		Instances sportsData = new Instances("category", sportsClassifierFv, 0);
		sportsData.setClassIndex(sportsData.numAttributes() - 1);
		
		// Create an instance for each classifier, using the given arguments
		double[] politicsTestValues = new double[politicsData.numAttributes()];
		politicsTestValues[0] = politicsData.attribute(0).addStringValue(tweet);
		politicsTestValues[1] = Utils.missingValue();
		
		double[] musicTestValues = new double[musicData.numAttributes()];
		musicTestValues[0] = musicData.attribute(0).addStringValue(tweet);
		musicTestValues[1] = Utils.missingValue();
		
		double[] sportsTestValues = new double[sportsData.numAttributes()];
		sportsTestValues[0] = sportsData.attribute(0).addStringValue(tweet);
		sportsTestValues[1] = Utils.missingValue();

		// Add instances to dataset
		politicsData.add(new DenseInstance(1.0, politicsTestValues));
		musicData.add(new DenseInstance(1.0, musicTestValues));
		sportsData.add(new DenseInstance(1.0, sportsTestValues));
		
		// Classify the instance
		double politicsValue = politicsClassifier.classifyInstance(politicsData.instance(0));
		String politicsPrediction = politicsData.classAttribute().value((int) politicsValue);
		
		double musicValue = musicClassifier.classifyInstance(musicData.instance(0));
		String musicPrediction = musicData.classAttribute().value((int) musicValue);
		
		double sportsValue = sportsClassifier.classifyInstance(sportsData.instance(0));
		String sportsPrediction = sportsData.classAttribute().value((int) sportsValue);
		
		//System.out.println("Predictions (Politics-Music-Sports): "+ politicsPrediction + "|" + musicPrediction + "|" + sportsPrediction);
		
		List<String> predictedCategories = new ArrayList<String>();
		
		if(politicsPrediction.equalsIgnoreCase("yes")) {
			predictedCategories.add("politics");	
		}
>>>>>>> 17235abe2b3218a98f8a94e7d6e0c4211e2aeaa1

			// Add instances to dataset
			politicsData.add(new DenseInstance(1.0, politicsTestValues));
			musicData.add(new DenseInstance(1.0, musicTestValues));
			sportsData.add(new DenseInstance(1.0, sportsTestValues));

			// Classify the instance
			double politicsValue = 0;
			politicsValue = politicsClassifier.classifyInstance(politicsData.instance(0));
			String politicsPrediction = politicsData.classAttribute().value((int) politicsValue);

			double musicValue = politicsClassifier.classifyInstance(musicData.instance(0));
			String musicPrediction = musicData.classAttribute().value((int) musicValue);

			double sportsValue = politicsClassifier.classifyInstance(sportsData.instance(0));
			String sportsPrediction = sportsData.classAttribute().value((int) sportsValue);

			//System.out.println("Predictions (Politics-Music-Sports): "+ politicsPrediction + "|" + musicPrediction + "|" + sportsPrediction);


			if(politicsPrediction.equalsIgnoreCase("yes")) {
				predictedCategories.add("politics");
			}

			if(musicPrediction.equalsIgnoreCase("yes")) {
				predictedCategories.add("music");
			}

			if(sportsPrediction.equalsIgnoreCase("yes")) {
				predictedCategories.add("sports");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		if(predictedCategories.size()>0)
		{
			// round | tweettime | id | country | retweetcount | tweet | userid
			this.collector.emit(new Values(
					tweet,
					tuple.getLongByField("round"),
					tuple.getLongByField("tweetTime"),
					tuple.getLongByField("id"),
					tuple.getLongByField("retweetcount"),
					tuple.getLongByField("userid"),
					tuple.getStringByField("country"),
					predictedCategories));

		}
<<<<<<< HEAD
=======
		
		return predictedCategories;
	}
	
	public static void main(String[] args) throws Exception {
		TweetCategoryPredictionBolt e = new TweetCategoryPredictionBolt();
		e.prepare();
		List<String> predictedCategories = e.execute("opposition leader bill shorten comfort senator nova peris speak departure politics");
		//System.out.println("Predicted Categories: " + predictedCategories);
>>>>>>> 17235abe2b3218a98f8a94e7d6e0c4211e2aeaa1
	}

//	public static void main(String[] args) throws Exception {
//		TweetCategoryPredictionBolt e = new TweetCategoryPredictionBolt();
//		e.prepare();
//		List<String> predictedCategories = e.execute("breaking find trump reach number delegate need clinch republican nomination president");
//		//System.out.println("Predicted Categories: " + predictedCategories);
//	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet", "round", "tweettime", "id", "retweetcount", "userid", "country", "categories"));
	}
}
