package weka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.meta.FilteredClassifier;
import weka.core.Instances;
import weka.filters.unsupervised.attribute.StringToWordVector;

public class TweetClassifier {
	static File resourcesDirectory = new File("src/main/resources");
	private static String inputTrainingFileName = "tweetcategory.txt";
	private static String inputTestFileName = "tweetcategorytest.txt";
	
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
	

	public static void main(String[] args) throws Exception {
		
		// TRAINING PHASE
		
		// Read train data
		BufferedReader trainingFile = readDataFile(inputTrainingFileName);
		
		Instances trainData = new Instances(trainingFile);
		trainData.setClassIndex(trainData.numAttributes() - 1);
		
		// To handle string attributes
		StringToWordVector filter = new StringToWordVector();
		filter.setAttributeNamePrefix("taylan");
		filter.setAttributeIndices("first-last");
		
		FilteredClassifier classifier = new FilteredClassifier(); 
		classifier.setFilter(filter); 
		classifier.setClassifier(new NaiveBayes());
		classifier.buildClassifier(trainData);
				
		
		// TESTING PHASE
		// Read test data
		BufferedReader testFile = readDataFile(inputTestFileName);
		
		Instances testData = new Instances(testFile);
		testData.setClassIndex(testData.numAttributes() - 1);
		
		// Predict each case
		for (int i = 0; i < testData.size(); i++) {
			double prediction = classifier.classifyInstance(testData.instance(i)); 
			System.out.println("Tweet: " + testData.instance(i).stringValue(0) + " | Prediction: " + testData.classAttribute().value((int) prediction));
		}
	}

}
