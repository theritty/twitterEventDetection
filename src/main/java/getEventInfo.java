import cassandraConnector.CassandraDao;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import java.util.*;

public class getEventInfo {
  public static void main(String[] args) throws Exception {
    CassandraDao cassandraDao = new CassandraDao("tweets", "counts", "events");
    ArrayList<Long> roundlist = new ArrayList<>();
    ResultSet resultSet;
    try {
      resultSet = cassandraDao.getRoundsFromEvents();

      Iterator<Row> iterator = resultSet.iterator();
      while(iterator.hasNext())
      {
        Row row = iterator.next();
        roundlist.add(row.getLong("round"));
      }
      Collections.sort(roundlist, new Comparator<Long>() {
        public int compare(Long m1, Long m2) {
          return m1.compareTo(m2);
        }
      });


    } catch (Exception e) {
      e.printStackTrace();
    }

    for(long r:roundlist)
    {
      writeInfo(cassandraDao,r,"USA");
      writeInfo(cassandraDao,r,"CAN");
    }
    return ;
  }

  private static void writeInfo(CassandraDao cassandraDao, long r, String country) throws Exception {
    ResultSet rsCAN = cassandraDao.getFromEvents(r,country);
    Iterator<Row> iteratorCAN = rsCAN.iterator();
    while (iteratorCAN.hasNext())
    {
      Row row = iteratorCAN.next();
      String word = row.getString("word");
        Date d = new Date(12*60*1000*r) ;
        System.out.println(d + " " + row.getString("country") + " " + word );
    }
  }
}
