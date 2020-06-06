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

public class WordCountImproved extends Configured implements Tool {

        public static class TokenizerMapper
        extends Mapper<LongWritable, Text, Text, LongWritable>{

                private final static LongWritable one = new LongWritable(1);
                private Text word = new Text();
                Pattern word_separator = Pattern.compile("[\\p{Punct}\\s]+");//Regex expression provided, based on which the input should be separated into words
		
		@Override
                public void map(LongWritable key, Text value, Context context
                                ) throws IOException, InterruptedException {
			
			String input_line  = value.toString().toLowerCase();//Converts all the characters in the input line to lowercase
                        String[] words = word_separator.split(input_line);//Splits the input line into words based on the regex expression described

                        for(int itr = 0; itr < words.length; itr++){
                                if(words[itr] != ""){
                                        word.set(words[itr]);
                                        context.write(word,one);
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
                        int sum = 0;
                        for (LongWritable val : values) {
                                sum += val.get();
                        }
                        result.set(sum);
                        context.write(key, result);
                }
        }

        public static void main(String[] args) throws Exception {
                int res = ToolRunner.run(new Configuration(), new WordCountImproved(), args);
                System.exit(res);
        }

        @Override
        public int run(String[] args) throws Exception {
                Configuration conf = this.getConf();
                Job job = Job.getInstance(conf, "word count");
                job.setJarByClass(WordCountImproved.class);

                job.setInputFormatClass(TextInputFormat.class);

                job.setMapperClass(TokenizerMapper.class);
                job.setCombinerClass(LongSumReducer.class);
                job.setReducerClass(LongSumReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(LongWritable.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                TextInputFormat.addInputPath(job, new Path(args[0]));
                TextOutputFormat.setOutputPath(job, new Path(args[1]));

                return job.waitForCompletion(true) ? 0 : 1;
        }
}
 
