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
  	// One is the value
		private IntWritable temp = new IntWritable();
		// word is the key
		private Text year = new Text();

		public void configure(JobConf job) {
		}

		// This is the map function
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	    // Takes a line of input and makes it a string
	    String line = value.toString();
      String[] ACCURACY = new String[] {"0","1","4","5","9"};

      year.set(line.substring(15,19));

      if (line.substring(88,92).equals("9999") || !(Arrays.asList(ACCURACY).contains(line.substring(92,93)))) {
        temp.set(-9999);
      } else if (line.substring(87,88).equals("+")) {
        temp.set(Integer.parseInt(line.substring(88,92)));
      } else {
        temp.set(Integer.parseInt(line.substring(87,92)));
      }

      output.collect(year, temp);
	    // Turns the string into discrete tokens
	    // StringTokenizer tokenizer = new StringTokenizer(line);
	    // Loops through tokens
	    // while (tokenizer.hasMoreTokens()) {
				// Sets the key to the next token
				// word.set(tokenizer.nextToken());
				// Sets the output to the token, and a value of one
				// output.collect(word, one);
	    // }
	  // finishes the loop and the mapping
		}
  }

  // Inner Reduce class
  public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		// This is the reduce function
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	  	// int max = Collections.max(values);
      // Sets initial max to -9999
      int max = -9000;
      while (values.hasNext()) {
        int current = values.next().get();
        if (current > max) {
          max = current;
        }
      }

      // Sets initial sum to zero
	    // int sum = 0;
	    // Loops through list of values
	    // while (values.hasNext()) {
	    	// Adds each value to the preexisting sum
				// sum += values.next().get();
	    // }
	    // Collects each key and the corresponding sum
	    output.collect(key, new IntWritable(max));
		}
  }

  // This is a run function that has some configuration stuff
  public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), Temperature.class);
		conf.setJobName("temperature");

		// Sets classes of keys, values
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