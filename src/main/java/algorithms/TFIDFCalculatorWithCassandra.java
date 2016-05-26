package algorithms;

import cass.CassandraDao;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import scala.util.parsing.combinator.testing.Str;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;


/**
 * Created by ceren on 05.04.2016.
 */
public class TFIDFCalculatorWithCassandra {
    private List<List<String>> docs;
    private List<String> doc;
    private String term;
    private UUID id = UUID.randomUUID();

    public void readFiles(ArrayList<Long> roundList, long roundNum, CassandraDao cassandraDao) {
        if (docs == null)
        {
            docs = new ArrayList<>();
        }

        for(long round : roundList)
        {
            List<String> new_list;
            try {

                new_list = readRound(round, cassandraDao) ;
                if(round == roundNum)
                {
                    doc = new_list;
                }
                docs.add(new_list);

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public ArrayList<String> readRound(long roundNum, CassandraDao cassandraDao) throws FileNotFoundException {
        ResultSet resultSet = cassandraDao.readRules("SELECT tweet FROM tweets3 WHERE round=" + roundNum + ";");
        Iterator<Row> iterator = resultSet.iterator();
        ArrayList<String> roundlist = new ArrayList<>();
        while(iterator.hasNext())
        {
            Row row = iterator.next();
            String tweet = row.getString("tweet");
            if(tweet == null) continue;
            String[] splittedList = tweet.split(" ");
            for(String s : splittedList)
                if(s != null || s.length() > 0)
                    roundlist.add(s);
        }

        return roundlist;
    }


    /**
     * @return term frequency of term in document
     */
    public double tf() {
        double result = 0;
        for (String word : doc) {
//            System.out.println("term " + term + " word " + word);
            if (term.equalsIgnoreCase(word) || word.equalsIgnoreCase("#" + term))
                result++;
        }

        if ( docs.size() == 0)
        {
            System.out.println("tf Nan");
            return Double.MAX_VALUE;
        }
        return result / doc.size();
    }

    /**
     * @return the inverse term frequency of term in documents
     */
    public double idf() {
        double n = 0;
        for (List<String> doc : docs) {
            for (String word : doc) {
                if (term.equalsIgnoreCase(word) || word.equalsIgnoreCase("#" + term)) {
                    n++;
                    break;
                }
            }
        }

        if ( n == 0)
        {
            System.out.println("idf Nan");
            return 0;
        }
        return Math.log(docs.size() / n);
    }

    /**
     * @return the TF-IDF of term
     */
    public double tfIdf(ArrayList<Long> rounds, String term, long roundNum, CassandraDao cassandraDao) {

//        System.out.println("---------------------------------");
        readFiles(rounds, roundNum, cassandraDao);
//        System.out.println("Files: " + files.toString());
//        System.out.println("File: " + currentFile);
        this.term = term;

        double tf = tf() ;
        double idf = idf();
        double result = tf * idf;
//        System.out.println( "Term: " + term + " current doc: " + currentFile + " tf: " + Double.toString(tf) +  " idf: " + Double.toString(idf) +" result " + result );


//        System.out.println("---------------------------------");
        return result;

    }




}