package demo.algorithms;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import demo.cass.CassandraDao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Created by ceren on 29.05.2016.
 */
public class CountCalculator {

  public HashMap<String, Long> addNewEntryToCassCounts(CassandraDao cassandraDao, String tweetTable, long round, String word, String country)
  {
    long count=0L, allcount=0L;
    HashMap<String, Long> counts = new HashMap<>();
    ResultSet resultSet2 = null;//.readRules("SELECT tweet FROM " + tweetTable + " WHERE round=" + round + ";");
    try {
      resultSet2 = cassandraDao.getTweetsByRound(round);
      Iterator<Row> iterator2 = resultSet2.iterator();

      while(iterator2.hasNext())
      {
        Row row = iterator2.next();
        String tweet = row.getString("tweet");
        if(tweet == null) continue;
        String[] splittedList = tweet.split(" ");
        for(String s : splittedList) {
          allcount++;
          if ((s != null || s.length() > 0) && (s.equals(word) || s.equals("#" + word))) {
            count++;
          }
        }
      }
      counts.put("count", count);
      counts.put("allcount", allcount);
      insertValuesToCass(cassandraDao, round, word, country, count, allcount);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return counts;
  }
  public HashMap<String, Double> getCountOfWord(CassandraDao cassandraDao, String tweetTable, String word, long round, String country) {
    double count=0L, allcount=0L;
    HashMap<String, Double> hm = null;
    try {
      ResultSet resultSet =cassandraDao.getFromCounts(round, word, country);

      Iterator<Row> iterator = resultSet.iterator();
      if (!iterator.hasNext()) {
        HashMap<String, Long> tmp = addNewEntryToCassCounts(cassandraDao, tweetTable, round, word, country);
        count = tmp.get("count");
        allcount = tmp.get("allcount");
      }
      else{
        Row row = iterator.next();
        if(row.getLong("count")<0 || row.getLong("totalnumofwords")<0 )
        {
          HashMap<String, Long> tmp = addNewEntryToCassCounts(cassandraDao, tweetTable, round, word, country);
          count = tmp.get("count");
          allcount = tmp.get("allcount");
        }
        else {
          count = row.getLong("count");
          allcount = row.getLong("totalnumofwords");
        }
      }

      hm = new HashMap<>();
      hm.put("count", count);
      hm.put("totalnumofwords", allcount);

    } catch (Exception e) {
      e.printStackTrace();
    }
    return hm;
  }

  private void insertValuesToCass(CassandraDao cassandraDao, long round, String word, String country, long count, long allcount)
  {
    try {
      List<Object> values = new ArrayList<>();
      values.add(round);
      values.add(word);
      values.add(country);
      values.add(count);
      values.add(allcount);
      cassandraDao.insertIntoCounts(values.toArray());
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
