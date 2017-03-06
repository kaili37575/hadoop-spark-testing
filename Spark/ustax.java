import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.regex.Pattern;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.List;
import java.util.ArrayList;

import scala.Tuple2;



public final class ustax {
 private static final Pattern SPACE = Pattern.compile(",");

 public static void main(String[] args) throws Exception {
 if (args.length < 2) {
 System.err.println("Usage: JavaWordCount <input> <output>");
 System.exit(1);
 }

 JavaSparkContext jsc = new JavaSparkContext();
 JavaRDD<String> lines = jsc.textFile(args[0]);
 JavaRDD<String> linee =lines.filter(x-> !x.contains("STATE"));//remove header
 JavaPairRDD<String, Integer> ones = linee.mapToPair(line -> {
   String[] sarr=SPACE.split(line);
   String[] num=sarr[4].split("\\.");

               int number=Integer.parseInt(num[0]);
               return new Tuple2<>(sarr[1]+","+sarr[3], number);


             });
 JavaPairRDD<String, Integer> counts = ones.reduceByKey( (count1, count2) ->
 (count1 + count2));
 counts.coalesce(1).sortByKey().saveAsTextFile(args[1]);
 jsc.stop();
 }
}
