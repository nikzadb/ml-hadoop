// *********************************************************************************************************
// *********************************************************************************************************
// Multiple Linear regression by gradient descent in Hadoop (many mappers-one reducer)
//
// Running
//     "bin/hadoop jar MultipleLinearRegressionMapReduce.jar com.ML_Hadoop.MultipleLinearRegression.MultipleLinearRegressionMapReduce ...
//                 <input_data_path> <output_path> <alpha, by default=0.1> <iteration, by default=100> ...
//                 <feature_size> <input_data_size>"
//     To stop confusion, it is recommended to remove "/user/hduser/LinearReg" before starting a new job by 
//     "bin/hadoop dfs -rmr "/user/hduser/LinearReg
//
// Inputs 
//     <input_data_path>  input text files/directory. Each line in the text file is as follows(https://gist.github.com/4202286):
//                        [yreal, feature-1, feature-2, ..., feature-N]
//                          151 , 0.038    , 0.05     , ..., 0.061 
//     alpha              The learning rate for gradient descent. Its value is between [0.001, 1]. By default its value is 0.1
//     iteration          The number of iterations. Its value is 100 by default.
//     feature_size       The number of features in input data. feature_size = (number of items in a line of input text file)-1.
//     input_data_size    The number of total lines in the input data. Values of "feature_size" and "input_data_size" are necessary 
//                        to upgrade regression coefficients (thata).
//
// Outputs 
//     "/user/hduser/LinearReg"  Keeps the history of values of regression coefficients (thata) for each iteration.
//                               Its format is [cost/error, theta-1, theta-2, ..., theta-N].
//
//     "/user/hduser/theta.txt"  Keeps the latest update of thata. Its format is [cost/error, theta-1, theta-2, ..., theta-N].
//
// Examples
//     "bin/hadoop jar MultipleLinearRegressionMapReduce.jar com.ML_Hadoop.MultipleLinearRegression.MultipleLinearRegressionMapReduce ...
//                 /user/hduser/diabetes /user/hduser/output-LR 0.1 3 11 442
//
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
//   08/02/2013  First Edition
// *********************************************************************************************************
// *********************************************************************************************************

package com.ML_Hadoop.MultipleLinearRegression;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
           
public class MultipleLinearRegressionMapReduce {
	  
	  public static void main(String[] args) throws Exception {
		  String[] theta;
		  int iteration = 0, num_of_iteration = 1;
		  int feature_size =0, input_data_size=0;
		  FileSystem fs;
		  Float alpha = 0.1f;
		  
		  do {
			  Configuration conf = new Configuration();
			  fs = FileSystem.get(conf);
	           
			  Job job = new Job(conf, "LinearRegressionMapReduce");
			  job.setJarByClass(MultipleLinearRegressionMapReduce.class);
			  
			  // the following two lines are needed for propagating "theta"
			  conf = job.getConfiguration();
			  
			  job.setOutputKeyClass(LongWritable.class);
			  job.setOutputValueClass(FloatWritable.class); 
		           
			  job.setMapperClass(MultipleLinearRegressionMap.class);
			  job.setReducerClass(MultipleLinearRegressionReduce.class);
		           
			  job.setInputFormatClass(TextInputFormat.class);
			  job.setOutputFormatClass(TextOutputFormat.class);
			  
			  job.setNumReduceTasks(1); // set mapred.reduce.tasks = 1 (only one reducer)
		  
			  FileInputFormat.addInputPath(job, new Path(args[0]));
			  Path out = new Path(args[1]);
			  if (fs.exists(out)) fs.delete(out, true);
			  
			  FileOutputFormat.setOutputPath(job, out);
			  alpha = Float.parseFloat(args[2]);
			  num_of_iteration = Integer.parseInt(args[3]);
			  feature_size = Integer.parseInt(args[4]);
			  input_data_size = Integer.parseInt(args[5]);
			  conf.setFloat("alpha", alpha);
			  conf.setInt("feature_size", feature_size);
			  conf.setInt("input_data_size", input_data_size);
			  conf.setInt("iteration", iteration);
			  
			  theta = new String[feature_size];
		       
			  if (iteration == 0){ // first iteration
				  for (int i=0; i< theta.length; i++)
					  theta[i]="0.0";
				  conf.setStrings("theta", theta);
			  }else{
				  try{
					  String uri = "/user/hduser/theta.txt";
					  fs = FileSystem.get(conf);
					  //FSDataInputStream in = fs.open(new Path(uri));
					  BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
					  theta = br.readLine().split(",");
				  }catch (Exception e){
					  
				  } 
				  conf.setStrings("theta", theta);
			  }
			  
			  for (int i=0; i<theta.length;i++)
		    		System.out.println("In MapRedce main function: theta[ "+i +" ]" + theta[i]);
			  
			  try {
				  job.waitForCompletion(true);
				  iteration++;
			  } catch (IOException e) {
				  e.printStackTrace();
			  }
		 } while (iteration < num_of_iteration);
		    
    
	  }
   }