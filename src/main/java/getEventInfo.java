import cassandraConnector.CassandraDao;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import topologyBuilder.TopologyHelper;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.*;

public class getEventInfo {
  public static class Event {
    public Event(String word, long round, String country)
    {
      this.word = word;
      this.round = round;
      this.country = country;
    }
    public String word;
    public long round;
    public String country;
  }

  public static ArrayList<Event> eventArrayListUSA = new ArrayList<>();
  public static ArrayList<Event> eventArrayListCAN = new ArrayList<>();
  public static void main(String[] args) throws Exception {
    Properties properties = new Properties();
    InputStream inputStream = TopologyHelper.class.getClassLoader().getResourceAsStream( "config.properties" );
    properties.load( inputStream );
    inputStream.close();
    String TWEETS_TABLE = properties.getProperty("tweets.table");
    String COUNTS_TABLE = properties.getProperty("counts.table");
    String EVENTS_TABLE = properties.getProperty("events.table");

    CassandraDao cassandraDao = new CassandraDao(TWEETS_TABLE, COUNTS_TABLE, EVENTS_TABLE);
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

    for(Event eventUSA:eventArrayListUSA)
    {
      for(Event eventCAN:eventArrayListCAN)
      {
        if(eventCAN.word.equals(eventUSA.word))
        {
          writeToFile("", "same_events_", "Event: " + eventCAN.word +
                  ", Canada timestamp: " + new Date(12*60*1000*eventCAN.round) +
                  ", America timestamp: " + new Date(12*60*1000*eventUSA.round));
        }
      }
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
      double incrementpercent = row.getDouble("incrementpercent");
      Date d = new Date(12*60*1000*r) ;

      Event event = new Event(word,r,country);
      if(country.equals("USA"))
        eventArrayListUSA.add(event);
      else
        eventArrayListCAN.add(event);

      if(incrementpercent<5)
      {
        writeToFile(country, "5-10_", "Round: " + r + ", date time:" + d + " Event: " + word);
      }
      else if( incrementpercent>=5 && incrementpercent<10)
      {
        writeToFile(country, "5-10_", "Round: " + r + ", date time:" + d + " Event: " + word);
      }
      else if( incrementpercent>=10 && incrementpercent<20)
      {
        writeToFile(country, "10-20_", "Round: " + r + ", date time:" + d + " Event: " + word);
      }
      else if( incrementpercent>=20 && incrementpercent<30)
      {
        writeToFile(country, "20-30_", "Round: " + r + ", date time:" + d + " Event: " + word);
      }
      else if( incrementpercent>=30 && incrementpercent<40)
      {
        writeToFile(country, "30-40_", "Round: " + r + ", date time:" + d + " Event: " + word);
      }
      else if( incrementpercent>=40 && incrementpercent<50)
      {
        writeToFile(country, "40-50_", "Round: " + r + ", date time:" + d + " Event: " + word);
      }
      else
      {
        writeToFile(country, "50+_", "Round: " + r + ", date time:" + d + " Event: " + word);
      }
      System.out.println(r + " " + d + " " + row.getString("country") + " " + word );
    }
  }

  public static void writeToFile(String country, String fileName, String line)
  {
    try {
      PrintWriter writer;
      writer = new PrintWriter("/home/ceren/Desktop/report/" + country + fileName + "_" + ".txt");
      writer.println(line);
      writer.close();

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }
}
