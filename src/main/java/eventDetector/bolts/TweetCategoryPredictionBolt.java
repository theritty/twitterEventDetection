package eventDetector.bolts;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.meta.FilteredClassifier;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.Utils;
import weka.filters.unsupervised.attribute.StringToWordVector;

public class TweetCategoryPredictionBolt {
	private FilteredClassifier politicsClassifier;
	private FilteredClassifier musicClassifier;
	private FilteredClassifier sportsClassifier;
	
	static File resourcesDirectory = new File("src/main/resources");
	private static String politicsTrainingFileName = "politicsTweets.txt";
	private static String musicTrainingFileName = "politicsTweets.txt";
	private static String sportsTrainingFileName = "politicsTweets.txt";
	
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
	
	public void prepare() throws Exception {
		// Read train data
		BufferedReader politicsTrainingFile = readDataFile(politicsTrainingFileName);
		BufferedReader musicTrainingFile = readDataFile(musicTrainingFileName);
		BufferedReader sportsTrainingFile = readDataFile(sportsTrainingFileName);
		
		Instances politicsTrainData = new Instances(politicsTrainingFile);
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
	}
	

	public List<String> execute(String tweet) throws Exception {

		// Create a dataset for each classifer, using the attributes
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

		if(musicPrediction.equalsIgnoreCase("yes")) {
			predictedCategories.add("music");
		}
		
		if(sportsPrediction.equalsIgnoreCase("yes")) {
			predictedCategories.add("sports");
		}
		
		return predictedCategories;
	}
	
	public static void main(String[] args) throws Exception {
		TweetCategoryPredictionBolt e = new TweetCategoryPredictionBolt();
		e.prepare();
		List<String> predictedCategories = e.execute("opposition leader bill shorten comfort senator nova peris speak departure politics");
		//System.out.println("Predicted Categories: " + predictedCategories);
	}

}
