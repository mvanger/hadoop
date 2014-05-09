import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;

import org.apache.hadoop.io.ArrayWritable;

	public class health_kmeans extends Configured implements Tool {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    	ArrayList<ArrayList<Double>>centers;
		ArrayList<Double> centroid_row;
        ArrayList<Double> data_point;
 	    Path[] localFiles = new Path[0];


 	   public void configure(JobConf conf) {
 	  		// Pulls data from DistributedCache into local memory
 		  String line;
 	  			try
 	  			{
 	  			localFiles = DistributedCache.getLocalCacheFiles(conf);
 	  			BufferedReader fileIn = new BufferedReader(new FileReader(localFiles[0].toString()));
 	  			centers = new ArrayList<ArrayList<Double>>();

 	  			while((line = fileIn.readLine()) != null){
 	 	  			centroid_row = new ArrayList<Double>();
 	 	  			String [] element_data = line.split("\\s+|\\t+");
 	  	   	  		for (int i = 2; i < element_data.length; i++){ //add elements starting from third column
 	  	   	  			centroid_row.add(Double.parseDouble(element_data[i]));
 	  	   	  		}
 	  	   	  		centers.add(centroid_row);
 	  				}
 	  			fileIn.close();
 	  			}
 	  			catch (IOException ioe)
 	  			{	System.err.println("IOException reading from distributed cache");
 	  				System.err.println(ioe.toString());	}

 	     }

  	    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)

    		throws IOException {


  	    	try {
  	    	data_point = new ArrayList<Double>();
  	    	String element_data [] = value.toString().split("\\s+|\\t+");
  	    	String myline = element_data[1] + " " + element_data[2] + " " + element_data[3] + " " + element_data[4] + " " + element_data[5] + " " + element_data[6] + " " + element_data[7] + " " + element_data[8] + " " + element_data[9];

  	    	for (int i = 1; i < element_data.length; i++){ //start from the 2 column, don't record npi
    	  		data_point.add(Double.parseDouble(element_data[i]));
    	  	}

  	    	double sumError = 99999999;
  		  	int clusterAssignment = -1;

  		  	for (int i=0; i<centers.size();i++){
  		  		double sum = 0;
  		  		for (int j=0;j<data_point.size();j++){
  		  			double intermediate_value = data_point.get(j)-centers.get(i).get(j);
  		  			sum += Math.pow(intermediate_value, 2);
  		  		}
  		  		if(sum < sumError){
  		  		clusterAssignment = i + 1;
		  		sumError = sum;
  		  		}
  		  	}

  	    	String outkey = Integer.toString(clusterAssignment);
   		  	output.collect(new Text(outkey), new Text (myline));
  	    	}
  	    	catch (ArrayIndexOutOfBoundsException e){
  	    	}

  	    	}
    }
    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

   	 public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
   		    throws IOException {

            int counter = 0;
            double col_1 = 0;
            double col_2 = 0;
            double col_3 = 0;
            double col_4 = 0;
            double col_5 = 0;
            double col_6 = 0;
            double col_7 = 0;
            double col_8 = 0;
            double col_9 = 0;

            while (values.hasNext()) {
              String[] points = values.next().toString().split("\\s+|\\t+");
              col_1 += Double.parseDouble(points[0]);
              col_2 += Double.parseDouble(points[1]);
              col_3 += Double.parseDouble(points[2]);
              col_4 += Double.parseDouble(points[3]);
              col_5 += Double.parseDouble(points[4]);
              col_6 += Double.parseDouble(points[5]);
              col_7 += Double.parseDouble(points[6]);
              col_8 += Double.parseDouble(points[7]);
              col_9 += Double.parseDouble(points[8]);
              counter ++;
            }

            String average_col_1 = String.valueOf(col_1 / counter);
            String average_col_2 = String.valueOf(col_2 / counter);
            String average_col_3 = String.valueOf(col_3 / counter);
            String average_col_4 = String.valueOf(col_4 / counter);
            String average_col_5 = String.valueOf(col_5 / counter);
            String average_col_6 = String.valueOf(col_6 / counter);
            String average_col_7 = String.valueOf(col_7 / counter);
            String average_col_8 = String.valueOf(col_8 / counter);
            String average_col_9 = String.valueOf(col_9 / counter);

            String newCentroid = average_col_1 + "\t" + average_col_2 + "\t" + average_col_3 + "\t" + average_col_4 + "\t" + average_col_5 + "\t" + average_col_6 + "\t" + average_col_7 + "\t" + average_col_8 + "\t" + average_col_9;

            output.collect(key, new Text(newCentroid));

   	   		  }
       }


	    public int run(String[] args) throws Exception {
	    	JobConf conf = new JobConf(getConf(), health_kmeans.class);
	    	conf.setJobName("health_kmeans");

	    	DistributedCache.addCacheFile(new Path("/user/huser28/health/health_centroids.txt").toUri(), conf);

	    	conf.setOutputKeyClass(Text.class);
	    	conf.setOutputValueClass(Text.class);

	    	conf.setMapperClass(Map.class);
	    	//conf.setCombinerClass(Reduce.class);
	    	conf.setReducerClass(Reduce.class);

	    	conf.setInputFormat(TextInputFormat.class);
	    	conf.setOutputFormat(TextOutputFormat.class);

	    	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	    	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	    	JobClient.runJob(conf);
	    	return 0;
	        }

	        public static void main(String[] args) throws Exception {
	    	int res = ToolRunner.run(new Configuration(), new health_kmeans(), args);
	    	System.exit(res);
	        }
    }

