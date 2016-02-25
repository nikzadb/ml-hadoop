// *********************************************************************************************************
// *********************************************************************************************************
// Reduce function of Multiple Linear regression by gradient descent in Hadoop
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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MultipleLinearRegressionReduce extends Reducer<LongWritable, FloatWritable, LongWritable, FloatWritable> {
	private Float[] theta;
	Configuration conf;
	private Float alpha, prediction_error;
	private int input_data_size, iteration;
	 
	@Override
    public void setup(Context context) {
		String[] temp = context.getConfiguration().getStrings("theta");
		theta = new Float[temp.length];
		for (int i=0; i<temp.length; i++)
			theta[i] = Float.parseFloat(temp[i]);
		alpha = context.getConfiguration().getFloat("alpha",0.1f);
		input_data_size = context.getConfiguration().getInt("input_data_size",1);
		iteration = context.getConfiguration().getInt("iteration",1);
	}
	
	@Override
	protected void cleanup(Context context) throws IOException{
		String uri = "/user/hduser/theta.txt";
        Path path = new Path( uri);

		
		try{
	        FileSystem fs = FileSystem.get(URI.create( uri), context.getConfiguration());
	        if (fs.exists(path)) fs.delete(path, true);
            BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));
            for (int i=0; i < theta.length; i++)
            	br.write(theta[i].toString()+",");
            br.write("\n");
            br.close();
		}catch(Exception e){
            System.out.println("File not found");
		}
		
		uri = "/user/hduser/LinearReg/theta-"+iteration+".txt";
        path = new Path(uri);
		
		try{
	        FileSystem fs = FileSystem.get(context.getConfiguration());
	        if (iteration == 0) 
	        	fs.delete(new Path("/user/hduser/LinearReg"), true);
	        OutputStreamWriter osw = new OutputStreamWriter(fs.create(path,true));
            BufferedWriter br=new BufferedWriter(osw);
			br.write(prediction_error + ", ");
            for (int i=0; i < theta.length; i++)
            	br.write(theta[i].toString()+", ");
            br.write("\n");
            br.close();
		}catch(Exception e){
            System.out.println("File not found");
		}
	}
	
	@Override // necessary otherwise it runs default reduce()
	public void reduce(LongWritable key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

    	// decode keys, key=0 is the cost and rest are values for theta
    	if ((int) key.get() == 0 ){ //key==new LongWritable(0)) is also correct
        	Float cost = 0.0f;
    		for (FloatWritable val : values)
    			cost += val.get();
    		prediction_error = cost;
   			context.write(key, new FloatWritable(cost));
    	} else { // extracts theta
        	Float cost = 0.0f;
    		for (FloatWritable val : values)
    			cost += val.get();   
    		
    		// update theta
    		System.out.println("cost for key: " + cost);
    		System.out.println("cost  "+ cost*alpha/input_data_size);

    		int key_index = (int) key.get()-1;
    		System.out.println("key_index: "+key_index);

    		theta[key_index] -= cost*alpha/input_data_size;
   			context.write(key, new FloatWritable(cost));
    	}
    	
      }
      
      
}