package testtwitter.twitterWordCount;

import scala.util.parsing.combinator.testing.Str;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;


/**
 * Created by ceren on 05.04.2016.
 */
public class TFIDFCalculator {
    private List<List<String>> docs;
    private List<String> doc;
    private String term;
    private UUID id = UUID.randomUUID();


    public void readFiles(ArrayList<String> fileList, String currentFile) {
        if (docs == null)
        {
            docs = new ArrayList<List<String>>();
        }
        for(String file : fileList)
        {
            List<String> new_list;
            try {

                new_list = readFile(file);
                if(file.equals(currentFile))
                {
                    doc = new_list;
                }
                docs.add(new_list);

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public ArrayList<String> readFile(String fileName) throws FileNotFoundException {
        Scanner s = new Scanner(new File(fileName));
        ArrayList<String> list = new ArrayList<>();
        while (s.hasNext()){
            list.add(s.next());
        }
        s.close();
        return list;
    }


    /**
     * @return term frequency of term in document
     */
    public double tf() {
        double result = 0;
        for (String word : doc) {
            if(word == null) continue;
            if (term.equalsIgnoreCase(word) || word.equalsIgnoreCase("#" + term))
                result++;
        }

        if ( docs.size() == 0)
        {
            System.out.println("tf Nan");
            return Double.MAX_VALUE;
        }
        return result / doc.size() ;
    }

    /**
     * @return the inverse term frequency of term in documents
     */
    public double idf() {
        double n = 0;
        for (List<String> doc : docs) {
            for (String word : doc) {
                if(word == null) continue;
                if (term.equalsIgnoreCase(word) || word.equalsIgnoreCase("#" + term)) {
                    n++;
                    break;
                }
            }
        }

        if ( n == 0)
        {
            System.out.println("idf Nan");
            return Double.MAX_VALUE;
        }
        return Math.log(docs.size() / n);
    }

    /**
     * @return the TF-IDF of term
     */
    public double tfIdf(ArrayList<String> files, String term, String currentFile) {

        docs = new ArrayList<List<String>>();

//        System.out.println("---------------------------------");
        readFiles(files, currentFile);
//        System.out.println("Files: " + files.toString());
//        System.out.println("File: " + currentFile);

        double tf = tf() ;
        double idf = idf();
        System.out.println("Term: " + term + " tf: " + Double.toString(tf) +  " idf: " + Double.toString(idf) );
        this.term = term;

        double result = tf * idf;

//        System.out.println("---------------------------------");
        return result;

    }



//    public static void main(String[] args) {
//
//        List<String> doc1 = Arrays.asList("Lorem", "ipsum", "dolor", "ipsum", "sit", "ipsum");
//        List<String> doc2 = Arrays.asList("Vituperata", "incorrupte", "at", "ipsum", "pro", "quo");
//        List<String> doc3 = Arrays.asList("Has", "persius", "disputationi", "id", "simul");
//        List<List<String>> documents = Arrays.asList(doc1, doc2, doc3);
//
//        TFIDFCalculator calculator = new TFIDFCalculator();
//        double tfidf = calculator.tfIdf(doc1, documents, "ipsum");
//        System.out.println("TF-IDF (ipsum) = " + tfidf);
//
//
//    }


}