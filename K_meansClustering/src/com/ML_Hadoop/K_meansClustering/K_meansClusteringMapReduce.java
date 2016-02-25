// *********************************************************************************************************
// *********************************************************************************************************
// K-means clustering in Hadoop (many mappers-one reducer)
// 
// Running
//     "bin/hadoop jar K_meansClusteringMapReduce.jar com.ML_Hadoop.K_meansClustering.K_meansClusteringMapReduce ...
//                 <input_data_path> <output_path> <# of clusters, by default=2> <# of iteration, by default=30> ...
//                 <# of features, by default=2>"
//
//     To stop confusion, it is recommended to remove "/user/hduser/K-means" and "/user/hduser/k_mean.txt" before starting a new job by 
//     "bin/hadoop dfs -rmr /user/hduser/K-means"
//
// Inputs 
//     <input_data_path>  input text files/directory. Each line in the text file is as follows 
//                        (a sample: http://cs.joensuu.fi/sipu/datasets/pathbased.txt  - note that the third column is labels):
//                        [yreal <TAB> feature-1 <TAB> feature-2 <TAB> ...<TAB> feature-N]
//                          151	   0.038	0.05	...		0.061 
//     # of clusters      The number of clusters/groups/labels in final clustering. By default its value is 2.
//     # of iteration     The number of iterations. Its value is 30 by default.
//     # of features      The number of features in input data. feature_size = (number of items in a line of input text file).
//
// Outputs 
//     "/user/hduser/K-means"     Keeps the history of values of clusters' centroids for each iteration.
//
//     "/user/hduser/k_mean.txt"  Keeps the latest update of centroids. 
//
// Examples
//     "bin/hadoop jar K_meansClusteringMapReduce.jar com.ML_Hadoop.K_meansClustering.K_meansClusteringMapReduce ...
//                 /user/hduser/pathbased.txt /user/hduser/output-KMEANS 3 4 2"
// Author
//   Nikzad Babaii Rizvandi <nikzad.b(at)gmail.com>
//
// License
//   The program is free to use for non-commercial academic purposes,
//   but for course works, you must understand what is going inside to use.
//   The program can be used, modified, or re-distributed for any purposes
//   if you or one of your group understand codes. Please note that the author
//   does not guarantee the code works properly for all applications; therefore 
//   the author does not accept any responsibility on any damage/lost by using
//   this code.
//
// Changes
//   11/02/2013  First Edition
// *********************************************************************************************************
// *********************************************************************************************************

package com.ML_Hadoop.K_meansClustering;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
           
public class K_meansClusteringMapReduce {
	  
	  public static void main(String[] args) throws Exception {
		  int iteration = 0, num_of_iteration = 30;
		  int feature_size =2;
		  FileSystem fs;
		  int number_of_clusters = 2;
		  
		  do {
			  Configuration conf = new Configuration();
			  fs = FileSystem.get(conf);
	           
			  Job job = new Job(conf, "K_meansClusteringMapReduce");
			  job.setJarByClass(K_meansClusteringMapReduce.class);
			  
			  conf = job.getConfiguration(); // This line is mandatory. 
			  
			  job.setOutputKeyClass(LongWritable.class);
			  job.setOutputValueClass(FloatArrayWritable.class);
		           
			  job.setMapperClass(K_meansClusteringMap.class);
			  job.setReducerClass(K_meansClusteringReduce.class);
		           
			  job.setInputFormatClass(TextInputFormat.class);
			  job.setOutputFormatClass(TextOutputFormat.class);
		  
			  job.setNumReduceTasks(1); // set number of reducers to one.
			  
			  FileInputFormat.addInputPath(job, new Path(args[0]));
			  Path out = new Path(args[1]);
			  if (fs.exists(out)) fs.delete(out, true);
			  
			  FileOutputFormat.setOutputPath(job, out);
			  number_of_clusters = Integer.parseInt(args[2]);
			  num_of_iteration = Integer.parseInt(args[3]);
			  feature_size = Integer.parseInt(args[4]);
			  
			  conf.setInt("number_of_clusters", number_of_clusters);
			  conf.setInt("feature_size", feature_size);
			  conf.setInt("current_iteration_num", iteration);
			  
			  try {
				  job.waitForCompletion(true);
				  iteration++;
			  } catch (IOException e) {
				  e.printStackTrace();
			  }
		 } while (iteration < num_of_iteration);
		    
    
	  }
   }