
import com.google.common.base.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class dadsa {
    static class Movie {

        private Integer rank;
        private String description;

        public Movie(Integer rank, String description) {
            super();
            this.rank = rank;
            this.description = description;
        }

        public Integer getRank() {
            return rank;
        }

        public String getDescription() {
            return description;
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("rank", rank)
                    .add("description", description)
                    .toString();
        }
    }
    public static String filterNumber(String number)
    {
        number = number.replaceAll("[^(0-9)]", "");
        return number;
    }
    public static String filterNumbers(Map map)
    {
        map.put("a","b");
        map.put("c","b");
       return  "aaaa";
    }
//
//    public static void ReadFile(String hdfs)throws IOException {
//        Configuration conf =new Configuration();
//        FileSystem fs = FileSystem.get(URI.create(hdfs),conf);
//        FSDataInputStream hdfsInStream = fs.open(new Path(hdfs));
//
//        byte[] ioBuffer =new byte[1024];
//        int readLen = hdfsInStream.read(ioBuffer);
//        while(readLen!=-1)
//        {
//            System.out.write(ioBuffer,0, readLen);
//            readLen = hdfsInStream.read(ioBuffer);
//        }
//        hdfsInStream.close();
//        fs.close();
//    }

    public static void main(String[] args) {
        List<String> movies = new ArrayList<String>();
        movies.add("1");
        movies.add("2");

        Map<Integer, String> mappedMovies = new HashMap<Integer, String>();
        for (String movie : movies) {
            mappedMovies.put(1, movie);
        }

        System.out.println("The Shawshank Redemption"+ mappedMovies.get(1));




// String you = "^&^&^you123$%$%你好";
//    String you = "^&^&^you123$%$%你好";
//    Map map = new HashMap<>();
//        System.out.println(map.size());
////    you = filterNumber(you);
////        System.out.println(you);
//        filterNumbers(map);
//
//        System.out.println(map.size());
//
//        SparkSession spark = SparkSession
//                .builder()
//                .appName("Java Spark SQL basic example")
//                .config("spark.some.config.option", "some-value")
//                .getOrCreate();

//        System.out.println(1==1);
        // creating a TreeSet
//        TreeSet<Integer> treeadd = new TreeSet<Integer>();
//
//        // adding in the tree set
//        treeadd.add(12);
//        treeadd.add(11);
//        treeadd.add(16);
//        treeadd.add(15);

        // getting the higher value for 13
//        System.out.println("Higher value of 13: "+treeadd.higher(15));
//        System.out.println("Higher value of 13: "+treeadd.lower(20));
//        System.out.println("Higher value of 13: "+treeadd.last());
//        System.out.println(System.currentTimeMillis()-1);
//        System.out.println(System.currentTimeMillis());
//        DateTimeFormatter dateTimeFormatter  = DateTimeFormatter.ofPattern("HHmmss");
//        DateTimeFormatter dateTimeFormatter  = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
//        System.out.println(LocalDateTime.parse("20180503", dateTimeFormatter).plusDays(-1).format(dateTimeFormatter));
//        System.out.println(LocalDateTime.parse("20180101000000", dateTimeFormatter).plusSeconds(30).format(dateTimeFormatter));
//        System.out.println(LocalTime.parse("000500", dateTimeFormatter).toSecondOfDay() - LocalTime.parse("000000", dateTimeFormatter).toSecondOfDay());

    }

}
