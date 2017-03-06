import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.regex.Pattern;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.List;
import java.util.ArrayList;

import scala.Tuple2;



public final class wordcount1 {
 private static final Pattern SPACE = Pattern.compile(" ");
 private static final Pattern pp=Pattern.compile("ing$|ness$|ed$|'s$|ly$|'$|--$|-$|\\)$|_$|;$|\\?$|!$|,$|:$");
 private static final Pattern p=Pattern.compile("^'|^\"|^\\(|^_");

 public static void main(String[] args) throws Exception {
 if (args.length < 2) {
 System.err.println("Usage: JavaWordCount <input> <output>");
 System.exit(1);
 }

 JavaSparkContext jsc = new JavaSparkContext();
 JavaRDD<String> lines = jsc.textFile(args[0]);
 JavaRDD<String> words=lines.flatMap(line->{
   String[] word=SPACE.split(line);
   List<String> list=new ArrayList<String>();
   for(int i=0;i<word.length;i++){
   String token=word[i].toLowerCase();
   Matcher matcher1=pp.matcher(token);
   String ss=matcher1.replaceAll("");
   Matcher matcher2=p.matcher(ss);
   String res=matcher2.replaceAll("");
   if (res!=null && res.length()>0)
   list.add(res);
 }
   return list.iterator();
 });
 //JavaRDD<String> linee =words.filter(x-> !x.contains(" "));//remove empty string
 JavaPairRDD<String, Integer> ones = words.mapToPair(word -> {

     return new Tuple2<>(word, 1);

             });
 JavaPairRDD<String, Integer> counts = ones.reduceByKey( (count1, count2) ->
 (count1 + count2));
 JavaPairRDD<Integer, String> swap=counts.coalesce(1).mapToPair(x->{return new Tuple2<>(x._2(),x._1());}).sortByKey(false);
 JavaPairRDD<String, Integer> swapback=swap.mapToPair(x->{return new Tuple2<>(x._2(),x._1());});
 swapback.saveAsTextFile(args[1]);
 jsc.stop();
 }
}
