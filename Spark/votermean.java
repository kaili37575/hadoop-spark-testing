import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.regex.Pattern;
import java.util.Arrays;
import java.util.regex.Matcher;

import scala.Tuple2;



public final class votermean {
 private static final Pattern SPACE = Pattern.compile(",");
 public static void main(String[] args) throws Exception {
 if (args.length < 2) {
 System.err.println("Usage: JavaWordCount <input> <output>");
 System.exit(1);
 }

 JavaSparkContext jsc = new JavaSparkContext();
 JavaRDD<String> lines = jsc.textFile(args[0]);
 JavaRDD<String> linee =lines.filter(x-> !x.contains("Code"));//remove header
 JavaPairRDD<String, Integer> ones = linee.mapToPair(line -> {
   String[] sarr=SPACE.split(line);
               int number=Integer.parseInt(sarr[3]);
               return new Tuple2<>(sarr[0], number);

             });
 JavaPairRDD<String, Integer> counts = ones.reduceByKey( (count1, count2) ->
 (count1 + count2));
 //saveAsTextFile(args[1]);
int cnt=(int)counts.count();

 JavaPairRDD<String, Integer> name=counts.coalesce(1).mapToPair(x->{
   return new Tuple2<>("MeanVoter",x._2());}).sortByKey(false);


JavaPairRDD<String, Integer> results = name.reduceByKey( (count1, count2) ->
             (count1 + count2));
             results.map(x->x._2()/cnt).saveAsTextFile(args[1]);

//JavaPairRDD<String, Integer> mean = res.reduceByKey( (count1, count2) ->
//             (count1 + count2)/2).coalesce(1).saveAsTextFile(args[1]);

 jsc.stop();
 }
}
