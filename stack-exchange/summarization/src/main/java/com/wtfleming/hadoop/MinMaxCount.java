package com.wtfleming.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MinMaxCount {

	public static class SEMinMaxCountMapper extends
			Mapper<Object, Text, Text, MinMaxCountTuple> {
		// Our output key and value Writables
		private Text outUserId = new Text();
		private MinMaxCountTuple outTuple = new MinMaxCountTuple();

		// This object will format the creation date string into a Date object
		private final static SimpleDateFormat frmt = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS");

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			// Parse the input string into a nice map
			Map<String, String> parsed = StackExchangeUtils.transformXmlToMap(value.toString());

			// Grab the "CreationDate" field since it is what we are finding
			// the min and max value of
			String strDate = parsed.get("CreationDate");

			// Grab the “UserID” since it is what we are grouping by
			String userId = parsed.get("UserId");

			// .get will return null if the key is not there
			if (strDate == null || userId == null) {
				// skip this record
				return;
			}

			try {
				// Parse the string into a Date object
				Date creationDate = frmt.parse(strDate);

				// Set the minimum and maximum date values to the creationDate
				outTuple.min = creationDate;
				outTuple.max = creationDate;

				// Set the comment count to 1
				outTuple.count = 1;

				// Set our user ID as the output key
				outUserId.set(userId);

				// Write out the user ID with min max dates and count
				context.write(outUserId, outTuple);
			} catch (ParseException e) {
				// An error occurred parsing the creation Date string
				// skip this record
			}
		}
	}

	public static class SEMinMaxCountReducer extends
			Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {
		private MinMaxCountTuple result = new MinMaxCountTuple();

		@Override
		public void reduce(Text key, Iterable<MinMaxCountTuple> values,
				Context context) throws IOException, InterruptedException {

			// Initialize our result
			result.min = null;
			result.max = null;
			int sum = 0;

			// Iterate through all input values for this key
			for (MinMaxCountTuple val : values) {

				// If the value's min is less than the result's min
				// Set the result's min to value's
				if (result.min == null || val.min.compareTo(result.min) < 0) {
					result.min = val.min;
				}

				// If the value's max is less than the result's max
				// Set the result's max to value's
				if (result.max == null || val.max.compareTo(result.max) > 0) {
					result.max = val.max;
				}

				// Add to our sum the count for val
				sum += val.count;
			}

			// Set our count to the number of input values
			result.count = sum;

			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: minmaxcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "StackOverflow Comment Date Min Max Count");
		job.setJarByClass(MinMaxCount.class);
		job.setMapperClass(SEMinMaxCountMapper.class);
		job.setCombinerClass(SEMinMaxCountReducer.class);
		job.setReducerClass(SEMinMaxCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MinMaxCountTuple.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class MinMaxCountTuple implements Writable {
		public Date min = new Date();
		public Date max = new Date();
		public long count = 0;

		private final static SimpleDateFormat frmt = new SimpleDateFormat(
				"yyyy-MM-dd'T'HH:mm:ss.SSS");


		@Override
		public void readFields(DataInput in) throws IOException {
			min = new Date(in.readLong());
			max = new Date(in.readLong());
			count = in.readLong();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeLong(min.getTime());
			out.writeLong(max.getTime());
			out.writeLong(count);
		}

		@Override
		public String toString() {
			return frmt.format(min) + "\t" + frmt.format(max) + "\t" + count;
		}
	}
}
