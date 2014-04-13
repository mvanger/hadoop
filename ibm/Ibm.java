import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

// This is the name of the outermost class
public class Ibm extends Configured implements Tool {

  // Inner Map class
  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    // These are the keys, values
    // value is the value
    private DoubleWritable valueCombo = new DoubleWritable();
    // keyCombo is the key
    private Text keyCombo = new Text();

    public void configure(JobConf job) {
    }

    // This is the map function
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
      // Takes a line of input and makes it a string
      String line = value.toString();
      // Splits the input on the commas
      String[] splits = line.split(",");

      // Takes the 30-33rd elements
      // Keeps them as strings, turns them into "ints"
      // For example, instead of "1.0", we have just "1"
      String combo1 = splits[29].substring(0,1);
      String combo2 = splits[30].substring(0,1);
      String combo3 = splits[31].substring(0,1);
      String combo4 = splits[32].substring(0,1);
      // Concatenates them
      String combo = combo1 + combo2 + combo3 + combo4;

      // Finds the value
      Double fourthColumn = Double.parseDouble(splits[3]);

      // Now we only want the key, value if the last column is false
      if (splits[splits.length-1].equals("false")) {
        keyCombo.set(combo);
        valueCombo.set(fourthColumn);
      }

      output.collect(keyCombo, valueCombo);
    }
  }

  // Inner Reduce class
  public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    // This is the reduce function
    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
      // Sets initial sum to zero
      double sum = 0;
      // Sets counter to zero
      int counter = 0;
      // Loops through list of values
      while (values.hasNext()) {
        // Adds each value to the preexisting sum
        sum += values.next().get();
        counter++;
      }

      // Gets the average
      double avg = sum/counter;
      // Collects each key and the corresponding sum
      output.collect(key, new DoubleWritable(avg));
    }
  }

  // This is a run function that has some configuration stuff
  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), Ibm.class);
    conf.setJobName("ibm");

    // Sets classes of keys, values
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(DoubleWritable.class);

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
    int res = ToolRunner.run(new Configuration(), new Ibm(), args);
    System.exit(res);
  }
}