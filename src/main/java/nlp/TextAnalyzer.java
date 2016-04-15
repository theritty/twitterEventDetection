package nlp;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

public class TextAnalyzer {
	
	private static StanfordCoreNLP pipeline;
	
	public TextAnalyzer() {
    	// Create a StanfordCoreNLP object
        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma");
        TextAnalyzer.pipeline = new StanfordCoreNLP(props);
	}

	// Takes a text, returns a lemmatized clean word list
    public static List<String> extractWordList(String text) {
    	// Get the custom stop word set
        CustomStopWords csw = new CustomStopWords();
        Set<String> stopWords = csw.getStopWords();
    	
        Annotation document = pipeline.process(text);
        List<String> extractedWords = new ArrayList<>();
        
        for(CoreMap sentence : document.get(SentencesAnnotation.class)){    
            for(CoreLabel token: sentence.get(TokensAnnotation.class)){       
                String word = token.word().toLowerCase().trim();
                
                // TODO: Check if it is a URL
                // TODO: Check if it is a number
                
                // If the word is nothing but a punctuation, ignore it
                if(Punctuation.removePunctuation(word).equalsIgnoreCase("")){
                	continue;
                }
              
                // Eliminate, if word is in stop word set
                else if (stopWords.contains(word)) {
                	continue;
                } 
                
                else {
                	String lemma = token.lemma().toLowerCase().trim();
                	Punctuation.removePunctuation(lemma);
                    extractedWords.add(lemma);
                }
            }
        }
        
        return extractedWords;
    }

    public static void main(String[] args) {
    	
        String text = "It was Ms. Fitzhugh. She was walking fast. "
       		+ "A strange expression crossed the faces of the students as they glanced toward "
       		+ "the door and saw the principal go straight into the boys’ restroom. The footsteps stopped."
       		+ " There was a deep, throaty sound difficult to describe. Then came an eruption of shrill "
       		+ "screaming and a rapid sound of heels. Moments later, Ms. Fitzhugh emerged, her eyes wild. "
       		+ "Screaming, she skidded in the hall and headed toward the office.";
       
      TextAnalyzer ta = new TextAnalyzer();
      System.out.println(ta.extractWordList(text));	   
        
    }
}
