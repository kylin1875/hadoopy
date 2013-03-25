package com.wtfleming.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.lang.StringEscapeUtils;


public class CommentWordCount {

    public static class SEWordCountMapper extends
                    Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                        throws IOException, InterruptedException {

            Map<String, String> parsed = 
                StackExchangeUtils.transformXmlToMap(value.toString());

            String txt = parsed.get("Text");
            if (null == txt) {
                return;  // skip this record
            }
            txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());
            txt = txt.replaceAll("'", "");
            txt = txt.replaceAll("[^a-zA-Z]", " ");

            StringTokenizer itr = new StringTokenizer(txt);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, ONE);
            }
        }
    }

    public static class IntSumReducer extends
                    Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                    Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                    .getRemainingArgs();

        if (2 != otherArgs.length) {
            System.err.println("Usage: commentwordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Stack Exchange comment word count");
        job.setJarByClass(CommentWordCount.class);
        job.setMapperClass(SEWordCountMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
