// *********************************************************************************************************
// *********************************************************************************************************
// Naive Bayes Classifier for continuous value features in Hadoop (many mappers-one reducer)
// (the distribution of each features is assumed to be Gaussian. Therefore, output of reduce function will be 
//  mean and variance of each feature.)
// 
// Running
//     "bin/hadoop jar NaiveBayesClassifierMapReduce_Continuous_Features.jar ... 
//                 com.ML_Hadoop.NaiveBayesClassifierMapReduce_Continuous_Features.NaiveBayesClassifierMapReduce_Continuous_Features ...
//                 <input_data_path> <output_path> <# of classes, by default=1> <# of features, by default=1>"
//
//     To stop confusion, it is recommended to remove "/user/hduser/naive_bayes_continuous.txt" before starting a new job by 
//     "bin/hadoop dfs -rmr /user/hduser/naive_bayes_continuous.txt"
//
// Inputs 
//     <input_data_path>  input text files/directory. Each line in the text file is as follows 
//                        (a sample: http://code.google.com/p/ml-hadoop/downloads/detail?name=naivebayesdata.txt&can=2&q=#makechanges)
//                        [class_id, feature-1, feature-2, ...,  feature-N]
//                         1,-0.134323,-0.124812,1,-0.495327,-0.296429,-0.980676,-0.3829,-1,-1 
//     # of classes       The number of classes/groups/labels in final training data. By default its value is 1.
//     # of features      The number of features in input data. feature_size = (number of items in a line of input text file).
//
// Outputs 
//     "/user/hduser/naive_bayes_continuous.txt"     Keeps <mean, variance> of each feature, computed from training data
//
// Examples
//     "bin/hadoop jar NaiveBayesClassifierMapReduce.jar ...
//      com.ML_Hadoop.NaiveBayesClassifier_Continuous_Features.NaiveBayesClassifierMapReduce_Continuous_Features ...
//      /user/hduser/naivebayesdata.txt /user/hduser/Naive_output 8 9"
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
//   18/02/2013  First Edition
// *********************************************************************************************************
// *********************************************************************************************************

package com.ML_Hadoop.NaiveBayesClassifier_Continuous_Features;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NaiveBayesClassifierMapReduce_Continuous_Features {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		int number_of_classes = 1;
		int number_of_features = 1;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
	           
		Job job = new Job(conf, "NaiveBayesClassifierMapReduce_Continuous_Features");
		job.setJarByClass(NaiveBayesClassifierMapReduce_Continuous_Features.class);
			  
   	    conf = job.getConfiguration(); // This line is mandatory. 

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(FloatArrayWritable.class); 
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(MapArrayWritable.class); 
		           
		job.setMapperClass(NaiveBayesClassifierMap_Continuous_Features.class);
		job.setReducerClass(NaiveBayesClassifierReduce_Continuous_Features.class);
		           
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(1);
		  
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path out = new Path(args[1]);
		if (fs.exists(out)) fs.delete(out, true);
		FileOutputFormat.setOutputPath(job, out);
		number_of_classes = Integer.parseInt(args[2]);
		number_of_features = Integer.parseInt(args[3]);
		conf.setInt("number_of_classes", number_of_classes);
		conf.setInt("number_of_features", number_of_features);
		
	    try {
	    	job.waitForCompletion(true);
			
	    } catch (IOException e) {
	    	e.printStackTrace();
	    }
	}

}
