import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Random;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

public class WordCount2 extends Configured implements Tool{

  public static class map_first
    extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String[] last={""};

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
          String itr=value.toString();
          Pattern p1=Pattern.compile("ing$|ness$|ed$|'s$|ly$|'$|--$|-$|\\)$|_$|;$|\\?$|!$|,$|:$");
          Pattern p2=Pattern.compile("^'|^\"|^\\(|^_");

          String[] str=itr.split(" ");
          String[] strr=new String[str.length];

          for(int i=0;i<str.length;i++){
            String token=str[i].toLowerCase();
            Matcher m1=p1.matcher(token);
            String ss=m1.replaceAll("");
            Matcher m2=p2.matcher(ss);
            strr[i]=m2.replaceAll("");
          }
          //remove null value in StringArray
          List<String> list = new ArrayList<String>();
          for(String s : strr){
            if(s!=null && s.length()>0){
              list.add(s);
            }
          }

          String[] nword=list.toArray(new String[list.size()]);
          if(nword.length>1){

          if(last[0].length()!=0){
            if(last[0].compareTo(nword[0])>0){
          word.set(nword[0]+" "+last[0]);
        }else{
          word.set(last[0]+" "+nword[0]);
        }

              context.write(word,one);
          }
          last[0]=nword[nword.length-1];
          for(int j=0;j<nword.length-1;j++){
            //System.out.println(strr[j]);
            if (nword[j].compareTo(nword[j+1])>0){
              String res=nword[j+1]+" "+nword[j];
              word.set(res);
            }else{
              String res=nword[j]+" "+nword[j+1];
              word.set(res);
                 }

            context.write(word,one);
          }
        }else if(nword.length==1){
          if(last[0].length()!=0){
            if(last[0].compareTo(nword[0])>0){
          word.set(nword[0]+" "+last[0]);
        }else{
          word.set(last[0]+" "+nword[0]);
        }
          context.write(word,one);
          last[0]=nword[nword.length-1];
        }
      }
      }
  }


  public static class reduce_first
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

private static class IntDescComparator extends Comparator{

public int compare(WritableComparable a,WritableComparable b){
  return -super.compare(a,b);
}
@Override
public int compare(byte[] b1, int s1,int l1,byte[] b2,int s2,int l2){
  return -super.compare(b1,s1,l1,b2,s2,l2);
}
  }


@Override
public int run(String[] args) throws Exception{
Configuration conf= getConf();
//set up a temp directory to store first-job's results
Path tempDir=new Path("wordcount1-temp-"+Integer.toString(new Random().nextInt(99999)));

@SuppressWarnings("deprecation")
Job job = Job.getInstance(conf, "word count1");
try{
job.setJarByClass(WordCount2.class);
job.setMapperClass(map_first.class);
job.setCombinerClass(reduce_first.class);
job.setReducerClass(reduce_first.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
job.setOutputFormatClass(SequenceFileOutputFormat.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, tempDir);

if(job.waitForCompletion(true)){

  Job job2=Job.getInstance(conf,"Sort");
  job2.setJarByClass(WordCount2.class);
  job2.setMapperClass(InverseMapper.class);//swap key and value
  job2.setInputFormatClass(SequenceFileInputFormat.class);
  //job2.setCombinerClass(reduce_second.class);
  //job2.setReducerClass(reduce_second.class);
  job2.setNumReduceTasks(1);
  job2.setSortComparatorClass(IntDescComparator.class);//do descing sort
  job2.setOutputKeyClass(IntWritable.class);
  job2.setOutputValueClass(Text.class);

  FileInputFormat.addInputPath(job2, tempDir);
  FileOutputFormat.setOutputPath(job2, new Path(args[1]));

  return job2.waitForCompletion(true) ? 0 : 1;



}else{
  return 0;
}
  }finally{FileSystem.get(conf).deleteOnExit(tempDir);//delete temp dir
  }
}
  public static void main(String[] args) throws Exception {
    int exitCode=ToolRunner.run(new Configuration(),new WordCount2(),args);
    System.exit(exitCode);
  }
}
