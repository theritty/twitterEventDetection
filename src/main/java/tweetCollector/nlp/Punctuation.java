package tweetCollector.nlp;

import java.util.regex.Pattern;

public class Punctuation {
	
	// TODO: Add these punctuations as well:[][(){}<>%]"  
	private static final Pattern PUNCTUATION = Pattern.compile("[,.:;!?+'&]");
	
	public static String removePunctuation(String text) {
		return PUNCTUATION.matcher(text).replaceAll("");
	}

	public static void main(String[] args) {
		// Test cases
		System.out.println(removePunctuation("."));
		System.out.println(removePunctuation(","));
		System.out.println(removePunctuation(";"));
		System.out.println(removePunctuation("..."));
		System.out.println(removePunctuation("?;"));
		System.out.println(removePunctuation("twitter_event"));
		System.out.println(removePunctuation("hello world"));
		System.out.println(removePunctuation("Dr.Pepper"));	
	}
}