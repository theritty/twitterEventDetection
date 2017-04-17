
package eventDetector.algorithms;

import java.io.Serializable;
import java.util.ArrayList;

public class CosineSimilarity implements Serializable {

    /**
     * Method to calculate cosine similarity between two documents.
     * @param docVector1 : document vector 1 (a)
     * @param docVector2 : document vector 2 (b)
     * @return
     */
    public static double cosineSimilarity(ArrayList<Integer> docVector1, ArrayList<Integer> docVector2) {
        double dotProduct = 0.0;
        double magnitude1 = 0.0;
        double magnitude2 = 0.0;
        double cosineSimilarity ;

        for (int i = 0; i < docVector1.size(); i++) //docVector1 and docVector2 must be of same length
        {
            dotProduct += docVector1.get(i) * docVector2.get(i);  //a.b
            magnitude1 += Math.exp(Math.log(docVector1.get(i))*2) ;
            magnitude2 += Math.exp(Math.log(docVector2.get(i))*2) ;
        }

        magnitude1 = Math.sqrt(magnitude1);//sqrt(a^2)
        magnitude2 = Math.sqrt(magnitude2);//sqrt(b^2)

        if (Math.abs(magnitude1) < 0.001 && Math.abs(magnitude2) < 0.001) {
            return 0.0;
        } else {
            cosineSimilarity = dotProduct / (magnitude1 * magnitude2);
        }
        return cosineSimilarity;
    }
}
