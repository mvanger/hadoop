import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Music1 extends Configured implements Tool {

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

		private static DoubleWritable duration = new DoubleWritable();
		private Text songartist = new Text();

		public void configure(JobConf job) {
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
		  String line = value.toString();
			String[] entry = line.split(",");
			String song = entry[0];
			String artist = entry[2];
			String out = entry[165];
			double length = Double.parseDouble(entry[3]);
			if (out.length() == 4) {
				int year = Integer.parseInt(out);
				if (year <= 2010 && year >= 2000) {
					songartist.set(song + "\t" + artist);
					duration.set(length);
					output.collect(songartist, duration);
				}
			}
		}
  }

  public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), Music1.class);
		conf.setJobName("Music1");

		conf.setOutputKeyClass(Text.class);
	  conf.setOutputValueClass(DoubleWritable.class);

		conf.setMapperClass(Map.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
  }

  public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Music1(), args);
		System.exit(res);
  }
}

