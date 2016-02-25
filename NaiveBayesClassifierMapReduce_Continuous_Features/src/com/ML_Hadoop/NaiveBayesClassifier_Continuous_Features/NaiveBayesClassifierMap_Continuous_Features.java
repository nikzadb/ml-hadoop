// *********************************************************************************************************
// *********************************************************************************************************
// Map function of Naive Bayes Classifier with Gaussian distribution in Hadoop
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NaiveBayesClassifierMap_Continuous_Features extends Mapper<LongWritable, Text, LongWritable, MapArrayWritable>{
	
	private Float[] features;
	private int class_id;
	private int number_of_classes, number_of_features; 
	private Map<Integer, ArrayList<Float[]>> features_probabilities = new HashMap<Integer,ArrayList<Float[]>>();
	private int[] num_of_members_in_each_class;

	@Override
	public void setup(Context context) {
		number_of_classes = context.getConfiguration().getInt("number_of_classes", 1);
		number_of_features = context.getConfiguration().getInt("number_of_features", 1);
		num_of_members_in_each_class = new int[number_of_classes];
	}
	
	@Override // is used as innermap to aggregate data before shuffling
	protected void cleanup(Context context) throws IOException, InterruptedException{
		//features_probabilities.put(class_id, features);
        Float[] sigma_x2 = new Float[number_of_features];
        Float[] sigma_x = new Float[number_of_features];
        Float[] mu_x_local = new Float[number_of_features];
        Float[] num_x_local = new Float[number_of_features];
        MapWritable[] map_output = new MapWritable[number_of_features];
        
        // It is a MUST to initilize all arrays before usage.
		for (int class_id=0; class_id < number_of_classes; class_id++){
			for (int i=0; i<number_of_features;i++){ 
	        	map_output[i] = new MapWritable(); // the way to initilize MapWritable[]
	        	sigma_x2[i] = 0.0f;
	            sigma_x[i] = 0.0f;
	            mu_x_local[i] = 0.0f;
	            num_x_local[i] = 0.0f;
	        }
			for (int member_id_in_a_class_id=0; member_id_in_a_class_id < num_of_members_in_each_class[class_id] ; member_id_in_a_class_id++){
				for (int feature_id_in_a_member_id=0; feature_id_in_a_member_id < number_of_features; feature_id_in_a_member_id++){
					sigma_x[feature_id_in_a_member_id] += (features_probabilities.get(class_id).get(member_id_in_a_class_id))[feature_id_in_a_member_id];
					sigma_x2[feature_id_in_a_member_id] += (features_probabilities.get(class_id).get(member_id_in_a_class_id))[feature_id_in_a_member_id]*((features_probabilities.get(class_id).get(member_id_in_a_class_id))[feature_id_in_a_member_id]);
				}
			}
			for (int feature_id_in_a_member_id=0; feature_id_in_a_member_id < number_of_features; feature_id_in_a_member_id++){
				num_x_local[feature_id_in_a_member_id] = (float)num_of_members_in_each_class[class_id];
				if (num_x_local[feature_id_in_a_member_id]==0)
					mu_x_local[feature_id_in_a_member_id] = 0.0f;
				else
					mu_x_local[feature_id_in_a_member_id] = sigma_x[feature_id_in_a_member_id]/num_x_local[feature_id_in_a_member_id];
			}
			
			for (int feature_id_in_a_member_id=0; feature_id_in_a_member_id < number_of_features; feature_id_in_a_member_id++){
				// key of MAP must be Writable (i.e., new Text("...")), but new string("...") is wrong.
				// value of MAP must be Writable or one subset !!! like FloatWritable
				map_output[feature_id_in_a_member_id].put(new Text("sigma_x"), new FloatWritable(sigma_x[feature_id_in_a_member_id]));
				map_output[feature_id_in_a_member_id].put(new Text("sigma_x2"), new FloatWritable(sigma_x2[feature_id_in_a_member_id]));
				map_output[feature_id_in_a_member_id].put(new Text("mu_x_local"), new FloatWritable(mu_x_local[feature_id_in_a_member_id]));
				map_output[feature_id_in_a_member_id].put(new Text("num_x_local"), new FloatWritable(num_x_local[feature_id_in_a_member_id]));
			}
			
			context.write(new LongWritable(class_id), new MapArrayWritable(map_output));
		}


	}
	
	@Override // necessary otherwise it runs default map()
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString(); 
		String[] features_str = line.split(","); // split text into separate strings 
		class_id = Integer.parseInt(features_str[0]);
		features = new Float[features_str.length-1];

		for (int i=0; i < features.length; i++){
			features[i] = Float.parseFloat(features_str[i+1]);
		}
		ArrayList<Float[]> t;
		if (features_probabilities.get(class_id) == null){
			t = new ArrayList<Float[]>();
		}else{
			t = features_probabilities.get(class_id);
		}
		t.add(features);
		features_probabilities.put(class_id, t);
		num_of_members_in_each_class[class_id]++;

	}
}
