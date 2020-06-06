import java.io.IOException;
import org.json.JSONObject;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class RedditAverage extends Configured implements Tool {

        public static class RedditMapper
        extends Mapper<LongWritable, Text, Text, LongPairWritable>{

                private final static LongPairWritable redditpair = new LongPairWritable();
                private Text word = new Text();
                private long redditscore;
		private final static long one = 1;

                @Override
                public void map(LongWritable key, Text value, Context context
                                ) throws IOException, InterruptedException {
                        String input = value.toString();
                        JSONObject record = new JSONObject(input);
                        String subreddit;
                        subreddit = (String) record.get("subreddit");
                        redditscore = Long.valueOf(record.get("score").toString());
                        word.set(subreddit);
                        redditpair.set(one, redditscore);
                        context.write(word, redditpair);

                }
        }


        public static class RedditCombiner
        extends Reducer<Text, LongPairWritable, Text, LongPairWritable>{
                private LongPairWritable average = new LongPairWritable();

                @Override
                public void reduce(Text key, Iterable<LongPairWritable> values,
                                Context context
                                ) throws IOException, InterruptedException {
                        long redditavg = 0;
                        long redditcount = 0 ;
                        for (LongPairWritable val: values){
                                redditcount += val.get_0();
                                redditavg += val.get_1();
                        }

                        average.set(redditcount, redditavg);
                        context.write(key, average);
                }
        }



        public static class RedditReducer
        extends Reducer<Text, LongPairWritable, Text, DoubleWritable>{
                private DoubleWritable average = new DoubleWritable();

                @Override
                public void reduce(Text key, Iterable<LongPairWritable> values,
                                Context context
                                ) throws IOException, InterruptedException {
                        double redditavg = 0;
                        long redditcount = 0;
                        for (LongPairWritable val: values){
                                redditcount += val.get_0();
                                redditavg += val.get_1();
                        }
                        redditavg = redditavg/redditcount;
                        average.set(redditavg);
                        context.write(key, average);
                }
        }

        public static void main(String[] args) throws Exception {
                int res = ToolRunner.run(new Configuration(), new RedditAverage(), args);
                System.exit(res);
        }

        @Override
        public int run(String[] args) throws Exception {
                Configuration conf = this.getConf();
                Job job = Job.getInstance(conf, "reddit average");
                job.setJarByClass(RedditAverage.class);

                job.setInputFormatClass(TextInputFormat.class);

                job.setMapperClass(RedditMapper.class);
                job.setCombinerClass(RedditCombiner.class);
                job.setReducerClass(RedditReducer.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(LongPairWritable.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DoubleWritable.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                TextInputFormat.addInputPath(job, new Path(args[0]));
                TextOutputFormat.setOutputPath(job, new Path(args[1]));

                return job.waitForCompletion(true) ? 0 : 1;
        }
}

                                                                                                                                                                                                                                                                                                                                                                                                                           
              
