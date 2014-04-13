import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

// This is the name of the outermost class
public class Temperature extends Configured implements Tool {

	// Inner Map class
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

  	// These are the keys, values
  	// temp is the value
		private IntWritable temp = new IntWritable();
		// year is the key
		private Text year = new Text();

		public void configure(JobConf job) {
		}

		// This is the map function
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	    // Takes a line of input and makes it a string
	    String line = value.toString();
      // This tells us that the temperature measurement was valid
      String[] ACCURACY = new String[] {"0","1","4","5","9"};

      // Sets the year key
      year.set(line.substring(15,19));

      // Checks for missing values (9999) or valid measurements
      if (line.substring(88,92).equals("9999") || !(Arrays.asList(ACCURACY).contains(line.substring(92,93)))) {
        // If invalid, sets sentinel value
        temp.set(-9999);
      // If +xxxx, takes last four numeric values
      } else if (line.substring(87,88).equals("+")) {
        temp.set(Integer.parseInt(line.substring(88,92)));
      // Otherwise takes -xxxx
      } else {
        temp.set(Integer.parseInt(line.substring(87,92)));
      }

      // Collects key, value
      output.collect(year, temp);
		}
  }

  // Inner Reduce class
  public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		// This is the reduce function
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	  	// This line does not work
      // int max = Collections.max(values);

      // Sets initial max to -9000
      int max = -9000;
      // Loops through values, checks if it is larger than the present max, and if so sets it to the new max
      while (values.hasNext()) {
        int current = values.next().get();
        if (current > max) {
          max = current;
        }
      }

	    // Collects each key and the corresponding max
	    output.collect(key, new IntWritable(max));
		}
  }

  // This is a run function that has some configuration stuff
  public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), Temperature.class);
		conf.setJobName("temperature");

    // Sets classes of keys, values (Mapper Output)
    // conf.setMapOutputKeyClass(IntWritable.class)
		// conf.setMapOutputValueClass(Text.class)

    // Sets classes of keys, values (Reducer Output)
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// Takes input and output paths
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		// Why does it return 0?
		return 0;
  }

  // Main function, calls the necessary things
  public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Temperature(), args);
		System.exit(res);
  }
}