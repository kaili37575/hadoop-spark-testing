import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class VoterMean {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, LongWritable>{

    private final static LongWritable one = new LongWritable(0);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                      String str= value.toString();
                      String[] sarr=str.split(",");
              if(sarr[0].equals("Code for district")){
                return;
              }else{
                            Long m=Long.parseLong(sarr[3]);
                            word.set("MeanVoter");
                            one.set(m);
                        context.write(word,one);
                      }

    }
  }

  public static class LongMeanReducer
       extends Reducer<Text,LongWritable,Text,LongWritable> {
    private LongWritable result = new LongWritable();

    public void reduce(Text key, Iterable<LongWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      long sum = 0;
      long cont = 0;
      for (LongWritable val : values) {
        cont=cont+1;
        sum += val.get();
      }
      result.set(sum/cont);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(VoterMean.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(LongMeanReducer.class);
    job.setReducerClass(LongMeanReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
