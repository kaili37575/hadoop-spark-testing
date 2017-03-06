import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.regex.Pattern;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.List;
import java.util.ArrayList;

import scala.Tuple2;



public final class wordcount2 {
 private static final Pattern SPACE = Pattern.compile(" ");
 private static final Pattern pp=Pattern.compile("ing$|ness$|ed$|'s$|ly$|'$|--$|-$|\\)$|_$|;$|\\?$|!$|,$|:$");
 private static final Pattern p=Pattern.compile("^'|^\"|^\\(|^_");

public void wordcount_pair(String[] args){
  String[] last={""};
  JavaSparkContext jsc = new JavaSparkContext();
  JavaRDD<String> lines = jsc.textFile(args[0],1);//read file use 1 partition to keep sequence of lines
  JavaRDD<String> words=lines.flatMap(line->{

    String[] word=SPACE.split(line);
    List<String> list=new ArrayList<String>();
    List<String> wordpair=new ArrayList<String>();


    for(int i=0;i<word.length;i++){
    String token=word[i].toLowerCase();
    Matcher matcher1=pp.matcher(token);
    String ss=matcher1.replaceAll("");
    Matcher matcher2=p.matcher(ss);
    String ress=matcher2.replaceAll("");
    if (ress!=null && ress.length()>0)
    list.add(ress);
 }

    String[] nword=list.toArray(new String[list.size()]);
    if(nword.length>1){

    if(last[0].length()!=0){
      if(last[0].compareTo(nword[0])>0){
    wordpair.add(nword[0]+" "+last[0]);
  }else{
    wordpair.add(last[0]+" "+nword[0]);

  }

    }
    last[0]=nword[nword.length-1];
    for(int j=0;j<nword.length-1;j++){
      String res="";
      //System.out.println(strr[j]);
      if (nword[j].compareTo(nword[j+1])>0){
        res=nword[j+1]+" "+nword[j];
        wordpair.add(res);
      }else{
        res=nword[j]+" "+nword[j+1];
        wordpair.add(res);
           }
    }
  }else if(nword.length==1){
    if(last[0].length()!=0){
      if(last[0].compareTo(nword[0])>0){
    wordpair.add(nword[0]+" "+last[0]);
  }else{
    wordpair.add(last[0]+" "+nword[0]);
  }
   last[0]=nword[nword.length-1];
  }
 }

 String[] array=wordpair.toArray(new String[wordpair.size()]);

    return wordpair.iterator();//return all word pair in one line
  });
  //JavaRDD<String> linee =words.filter(x-> !x.contains(" "));//remove empty string
  JavaPairRDD<String, Integer> ones = words.mapToPair(word -> {

      return new Tuple2<>(word, 1);

              });
  JavaPairRDD<String, Integer> counts = ones.reduceByKey( (count1, count2) ->
  (count1 + count2));
  //swap key and value to sortbyvalue
  JavaPairRDD<Integer, String> swap=counts.coalesce(1).mapToPair(x->{return new Tuple2<>(x._2(),x._1());}).sortByKey(false);
  JavaPairRDD<String, Integer> swapback=swap.mapToPair(x->{return new Tuple2<>(x._2(),x._1());});
  swapback.saveAsTextFile(args[1]);
  jsc.stop();
  }


 public static void main(String[] args) throws Exception {

 if (args.length < 2) {
 System.err.println("Usage: JavaWordCount <input> <output>");
 System.exit(1);
 }
wordcount2 method=new wordcount2();
method.wordcount_pair(args);

}
}
