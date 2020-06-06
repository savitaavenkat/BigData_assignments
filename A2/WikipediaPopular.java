import java.io.IOException;
import java.util.*;
import java.io.*;
import java.util.regex.*;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WikipediaPopular extends Configured implements Tool {

        public static class TokenizerMapper
        extends Mapper<LongWritable, Text, Text, LongWritable>{

                private Text word = new Text();

                @Override
                public void map(LongWritable key, Text value, Context context
                                ) throws IOException, InterruptedException{

                        String input = value.toString();
                        String[] words = new String[5];

                        words = input.split("\\s+");

                        String key_reducer = words[0];
                        String page_language = words[1];
                        String page_title = words[2];
                        String page_count = words[3];

                        LongWritable count = new LongWritable(Long.valueOf(page_count));

                        if (page_language.equals("en")) {
                                if((!page_title.equalsIgnoreCase("Main_Page")) && (!page_title.startsWith("Special:"))){
                                word.set(key_reducer);
                                context.write(word, count);

                                }

                        }
                }
        }

        public static class LongSumReducer
        extends Reducer<Text, LongWritable, Text, LongWritable> {
                private LongWritable result = new LongWritable();

                @Override
                public void reduce(Text key, Iterable<LongWritable> values,
                                Context context
                                ) throws IOException, InterruptedException {

                        long temp_count = 0;

                        for (LongWritable val : values) {
                                if(temp_count < val.get()){
                                        temp_count = val.get();
                                }
                        }

                        result.set(temp_count);
                        context.write(key, result);
                }
        }

        public static void main(String[] args) throws Exception {

                int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
                System.exit(res);
        }

        @Override
        public int run(String[] args) throws Exception {

                Configuration conf = this.getConf();
                Job job = Job.getInstance(conf, "wikipedia popular");
                job.setJarByClass(WikipediaPopular.class);

                job.setInputFormatClass(TextInputFormat.class);

                job.setMapperClass(TokenizerMapper.class);
                job.setReducerClass(LongSumReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(LongWritable.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                TextInputFormat.addInputPath(job, new Path(args[0]));
                TextOutputFormat.setOutputPath(job, new Path(args[1]));

                return job.waitForCompletion(true) ? 0 : 1;
        }
}

