package demo.algorithms;

import demo.cass.CassandraDao;
import java.util.*;


public class TFIDFCalculatorWithCassandra {

    /**
     * @return term frequency of term in document
     */
    public double tf(CassandraDao cassandraDao, long round, String country, String term) {
        CountCalculator countCalculator = new CountCalculator();
        HashMap<String, Long> hm = countCalculator.getCountOfWord(cassandraDao, term, round, country);
        if(hm == null) return 0;
        return hm.get("count") / hm.get("totalnumofwords");
    }

    /**
     * @return the inverse term frequency of term in documents
     */
    public double idf(CassandraDao cassandraDao, ArrayList<Long> rounds, String country, String term) {

        double wordCount=0L, totalNumOfWords=0L;
        for (long r : rounds) {
            CountCalculator countCalculator = new CountCalculator();
            HashMap<String, Long> hm = countCalculator.getCountOfWord(cassandraDao, term, r, country);
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
        double idf = idf(cassandraDao, rounds, country,term);
        double result = tf * idf;
        return result;

    }




}