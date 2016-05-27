package bolts.CommonBolts;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

/**
 * Created by ceren on 14.05.2016.
 */
public class RoundInfo{
    HashMap<String, Long> wordCounts;
    HashMap<String, Long> hashtagCounts;

    ArrayList<Date> dates;
    int roundCheckInBits = 0;
    public boolean emittedBefore = false;
    public RoundInfo()
    {
        wordCounts = new HashMap<>();
        hashtagCounts = new HashMap<>();
    }
    public HashMap<String, Long> getWordCounts() {
        return wordCounts;
    }
    public HashMap<String, Long> getHashtagCounts() {
        return hashtagCounts;
    }
    public void putWord(String word, Long count)
    {
        wordCounts.put(word, count);
    }
    public void setWordBlockEnd()
    {
        roundCheckInBits = roundCheckInBits | 1;
    }
    public void setEmittedBefore()
    {
        emittedBefore = true;
    }
    public boolean getEmittedBefore()
    {
        return emittedBefore;
    }
    public void putHashtag(String word, Long count)
    {
        hashtagCounts.put(word, count);
    }
    public void setHashtagBlockEnd()
    {
        roundCheckInBits = roundCheckInBits | 2;
    }
    public boolean isEndOfRound()
    {
        return roundCheckInBits == 3;
    }
    public int getRoundCheckInBits()
    {
        return roundCheckInBits;
    }
}