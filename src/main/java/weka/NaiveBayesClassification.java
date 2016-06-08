package weka;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class NaiveBayesClassification {

  static File resourcesDirectory = new File("src/main/resources");
  private static String politicsTrainingFileName = "politicsTweets.txt";
  private static String musicTrainingFileName = "musicTweets.txt";
  private static String sportsTrainingFileName = "sportsTweets.txt";

  private BaseNaiveBayesClassifier politicsClassifier;
  private BaseNaiveBayesClassifier musicClassifier;
  private BaseNaiveBayesClassifier sportsClassifier;

  public void prepare() {
    politicsClassifier = new BaseNaiveBayesClassifier();
    musicClassifier = new BaseNaiveBayesClassifier();
    sportsClassifier = new BaseNaiveBayesClassifier();

    try {
      politicsClassifier.createClassifier(politicsTrainingFileName, "tweet", "politics", "politics");
      musicClassifier.createClassifier(musicTrainingFileName, "tweet", "music", "music");
      sportsClassifier.createClassifier(sportsTrainingFileName, "tweet", "sports", "sports");
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }


  public ArrayList<String> execute(List<String> tweet_words) {
    String tweet = "";
    ArrayList<String> predictedCategories = new ArrayList<>();

    try {
      if(tweet_words== null || tweet_words.size()==0) {
        return predictedCategories;
      }

      for(String twee:tweet_words) {
        tweet = tweet + twee + " ";
      }

      String politicsPrediction = politicsClassifier.classify(tweet, "tweet", "politics");
      String musicPrediction = musicClassifier.classify(tweet, "tweet", "music");
      //String musicPrediction = "nope";
      String sportsPrediction = sportsClassifier.classify(tweet, "tweet", "sports");
      //String sportsPrediction = "nope";

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

    return predictedCategories;

  }

  public static void main(String[] args) throws Exception {
    NaiveBayesClassification e = new NaiveBayesClassification();
    e.prepare();
    List<String> deneme = new ArrayList<>();

    // Test1: music and politics
    deneme.add("alone");
    deneme.add("unplug");
    deneme.add("spellbinding");
    deneme.add("acoustic");
    deneme.add("performance");
    deneme.add("music");
    deneme.add("biggest");
    deneme.add("icon");

    // Try uncommenting the keywords below :)
//	    deneme.add("republican");
//	    deneme.add("president");
//	    deneme.add("trump");
//	    deneme.add("election");


    // Test2: other
//	    deneme.add("not");
//	    deneme.add("mystique");
//	    deneme.add("inject");
//	    deneme.add("guard");
//	    deneme.add("excess");
//	    deneme.add("iron");
//	    deneme.add("magneto");
//	    deneme.add("escape");
//	    deneme.add("be");
//	    deneme.add("talk");
//	    deneme.add("natural");
//	    deneme.add("body");
//	    deneme.add("iron");
    List<String> predictedCategories = e.execute(deneme);
    System.out.println("Predicted Categories: " + predictedCategories);
  }

}

