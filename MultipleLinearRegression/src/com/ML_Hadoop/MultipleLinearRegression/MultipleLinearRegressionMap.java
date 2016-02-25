// *********************************************************************************************************
// *********************************************************************************************************
// Map function of Multiple Linear regression by gradient descent in Hadoop
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
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultipleLinearRegressionMap extends Mapper<LongWritable, Text, LongWritable, FloatWritable>{

	FileInputStream fis = null;
	BufferedInputStream bis = null;
	private Float[] theta;
	private ArrayList<ArrayList<Float>> prediction_error = new ArrayList<ArrayList<Float>>();
	
	@Override
	public void setup(Context context) {
			String[] temp = context.getConfiguration().getStrings("theta");
			theta = new Float[temp.length];
			for (int i=0; i<temp.length; i++){
				theta[i] = Float.parseFloat(temp[i]);
			}
	} 
	
	@Override // is used as innermap to aggregate data before shuffling
	protected void cleanup(Context context) throws IOException, InterruptedException{
		// aggregate results from the same map and then send to reducers
		Float[] temp = new Float[theta.length+1];
		for (int i=0; i < temp.length; i++) temp[i] = 0.0f;

		for (int i=0; i < prediction_error.size(); i++) // iterates on rows
			for (int j=0; j < prediction_error.get(i).size(); j++){ // iterates on columns
				temp[j] += prediction_error.get(i).get(j);
		}
		
		for (int i=0; i < temp.length; i++)
			context.write(new LongWritable(i), new FloatWritable(temp[i]));
	}
	
	@Override // necessary otherwise it runs default map()
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString(); 
		String[] features = line.split(","); // split text into two texts: x, y
		ArrayList<Float> values = new ArrayList<Float>(); 
		
		for (int i=0; i<features.length; i++)
			values.add(new Float(features[i]));
		
		prediction_error.add(compute_J(values));
	}
	
	private ArrayList<Float> compute_J(ArrayList<Float> values){
		
		// calculate prediction for each record in values
		ArrayList<Float> result = new ArrayList<Float>();
		float y = values.get(0);
		float h_theta = 0.0f;
		
		// compute h_theta for current line of data (one line)
		for (int i=0; i < values.size(); i++){
			if (i==0)
				h_theta += theta[0];
			else 
				h_theta += theta[i]*values.get(i);
		}
		
		
		// calculate J(theta) or prediction error for current line of data
		float J_theta = (h_theta-y)*(h_theta-y);
		result.add(J_theta);

		// compute dJ(theta)/d(theta) for current line of data: derivative
		for (int j=0; j < values.size(); j++){
			if (j==0)
				result.add(h_theta-y);
			else
				result.add((h_theta-y)*values.get(j)); 
		}
		return result;
	}

}
