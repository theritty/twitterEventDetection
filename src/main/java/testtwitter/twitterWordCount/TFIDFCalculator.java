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


    public void readFiles(String fileNum, ArrayList<String> fileList, String currentFile) {
        if (docs == null)
        {
            docs = new ArrayList<List<String>>();
        }

        for(String file : fileList)
        {
            List<String> new_list;
            try {

                new_list = readFile( fileNum + "/" + file);
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
    public double tfIdf(String fileNum, ArrayList<String> files, String term, String currentFile) {

//        System.out.println("---------------------------------");
        readFiles(fileNum, files, currentFile);
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