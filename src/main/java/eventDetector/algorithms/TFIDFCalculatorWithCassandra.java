package eventDetector.algorithms;

import cassandraConnector.CassandraDao;

import java.util.ArrayList;
import java.util.HashMap;


public class TFIDFCalculatorWithCassandra {

    /**
     * @return term frequency of term in document
     */
    public double tf(CassandraDao cassandraDao, long round, String country, String term) {
        CountCalculator countCalculator = new CountCalculator();
        HashMap<String, Double> hm = countCalculator.getCountOfWord(cassandraDao, term, round, country);
        if(hm == null) return 0;
        if(hm.get("totalnumofwords") == 0)
            System.out.println("Term " + term + " total num zero");
        return hm.get("count") / hm.get("totalnumofwords");
    }

    /**
     * @return the inverse term frequency of term in documents
     */
    public double idf(CassandraDao cassandraDao, ArrayList<Long> rounds, String country, String term) {

        double wordCount=0L, totalNumOfWords=0L;
        for (long r : rounds) {
            CountCalculator countCalculator = new CountCalculator();
            HashMap<String, Double> hm = countCalculator.getCountOfWord(cassandraDao, term, r, country);
            if(hm == null) return 0;
            wordCount += hm.get("count");
            totalNumOfWords += hm.get("totalnumofwords");
        }
        return Math.log(totalNumOfWords / wordCount);
    }

    /**
     * @return the TF-IDF of term
     */
    public double tfIdf(CassandraDao cassandraDao, ArrayList<Long> rounds, String term, long roundNum, String country) {
        double tf = tf(cassandraDao, roundNum, country, term) ;
        if(tf == 0)  return 0;
        double idf = idf(cassandraDao, rounds, country,term);
        double result = tf * idf;
        return result;
    }




}