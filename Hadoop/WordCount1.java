import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Random;

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

public class WordCount1 extends Configured implements Tool{

  public static class map_first
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
          String itr = value.toString();
          Pattern pp=Pattern.compile("ing$|ness$|ed$|'s$|ly$|'$|--$|-$|\\)$|_$|;$|\\?$|!$|,$|:$");
          Pattern p=Pattern.compile("^'|^\"|^\\(|^_");

          String[] str=itr.split(" ");
          for(int i=0;i<str.length;i++) {
          String token=str[i].toLowerCase();
          Matcher matcher1=pp.matcher(token);
          String ss=matcher1.replaceAll("");
          Matcher matcher2=p.matcher(ss);
          String res=matcher2.replaceAll("");
          if (res.length()!=0){
              String[] splitword=res.split(" ");
              for (int j=0;j<splitword.length;j++){
          word.set(splitword[j]);
          context.write(word, one);
              }
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
job.setJarByClass(WordCount1.class);
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
  job2.setJarByClass(WordCount1.class);
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
}finally{FileSystem.get(conf).deleteOnExit(tempDir);//delete temp dir}
  }
}
  public static void main(String[] args) throws Exception {
    int exitCode=ToolRunner.run(new Configuration(),new WordCount1(),args);
    System.exit(exitCode);
  }
}
