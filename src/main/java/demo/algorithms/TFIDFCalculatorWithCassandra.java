package demo.algorithms;

import demo.cass.CassandraDao;
import java.util.*;


public class TFIDFCalculatorWithCassandra {

    /**
     * @return term frequency of term in document
     */
    public double tf(long round, String country, String term) {
        CountCalculator countCalculator = new CountCalculator();
        HashMap<String, Long> hm = countCalculator.getCountOfWord(term, round, country);
        return hm.get("count") / hm.get("totalnumofwords");
    }

    /**
     * @return the inverse term frequency of term in documents
     */
    public double idf(ArrayList<Long> rounds, String country, String term) {

        double wordCount=0L, totalNumOfWords=0L;
        for (long r : rounds) {
            CountCalculator countCalculator = new CountCalculator();
            HashMap<String, Long> hm = countCalculator.getCountOfWord(term, r, country);
            wordCount += hm.get("count");
            totalNumOfWords += hm.get("totalnumofwords");
        }
        return Math.log(totalNumOfWords / wordCount);
    }

    /**
     * @return the TF-IDF of term
     */
    public double tfIdf(ArrayList<Long> rounds, String term, long roundNum, String country) {
        double tf = tf(roundNum, country, term) ;
        double idf = idf(rounds, country,term);
        double result = tf * idf;
        return result;

    }




}