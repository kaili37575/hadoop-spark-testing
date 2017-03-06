import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class UStax {

  public static class map_UStax
    extends Mapper<Object, Text, Text, LongWritable>{

    private final static LongWritable one = new LongWritable(0);
    private Text word = new Text();
    private String last="";

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException{
          String str=value.toString();
          String[] sarr=str.split(",");
          if(sarr[1].equals("STATE") || sarr[4].equals("N1")){
            return;
          }else{
          String[] num=sarr[4].split("\\.");

                Long m=Long.parseLong(num[0]);
                word.set(sarr[1]+","+sarr[3]);
                one.set(m);
                context.write(word,one);
              }

      }
  }


  public static class LongSumReducer
       extends Reducer<Text,LongWritable,Text,LongWritable> {
    private LongWritable result = new LongWritable();

    public void reduce(Text key, Iterable<LongWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (LongWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      System.out.println(key+"+++++++"+sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf= new Configuration();
    Job job = Job.getInstance(conf, "US tax");

    job.setJarByClass(UStax.class);
    job.setMapperClass(map_UStax.class);
    job.setCombinerClass(LongSumReducer.class);
    job.setReducerClass(LongSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit( job.waitForCompletion(true) ? 0 : 1);
  }
}
