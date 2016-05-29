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

  public HashMap<String, Long> getCountOfWord(String word, long round, String country) {
    long count=0L, allcount=0L;
    HashMap<String, Long> hm = null;
    try {
      CassandraDao cassandraDao = new CassandraDao();

    ResultSet resultSet = cassandraDao.readRules("SELECT count FROM counts WHERE round=" + round +
            " AND word=" + word + "AND country=" + country + ";");

    Iterator<Row> iterator = resultSet.iterator();
    if (!iterator.hasNext() || iterator.next().getLong("count")<0 ||
            iterator.next().getLong("totalnumofwords")<0 ) {
      ResultSet resultSet2 = cassandraDao.readRules("SELECT tweet FROM tweets3 WHERE round=" + round + ";");
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
      insertValuesToCass(round, word, country, count, allcount);
    }
    else{
      Row row = iterator.next();
      count = row.getLong("count");
      allcount = row.getLong("totalnumofwords");
    }

    hm = new HashMap<>();
    hm.put("count", count);
    hm.put("totalnumofwords", allcount);

    } catch (Exception e) {
      e.printStackTrace();
    }
    return hm;
  }

  private void insertValuesToCass(long round, String word, String country, long count, long allcount)
  {
    try {
      CassandraDao cassandraDao = new CassandraDao();
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
