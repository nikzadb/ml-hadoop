// *********************************************************************************************************
// *********************************************************************************************************
// Reduce function of Naive Bayes Classifier with Gaussian distribution in Hadoop
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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NaiveBayesClassifierReduce_Continuous_Features extends Reducer<LongWritable, MapArrayWritable, LongWritable, FloatWritable> {
	
	private int number_of_classes, number_of_features;
	private ArrayList<MapWritable[]> probablity_info_output = new ArrayList<MapWritable[]>();
	
	@Override
    public void setup(Context context) {
		number_of_classes = context.getConfiguration().getInt("number_of_classes", 1);
		number_of_features = context.getConfiguration().getInt("number_of_features", 1);
		for (int i=0; i < number_of_classes; i++){
			probablity_info_output.add(null);
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException{
		String uri = "/user/hduser/naive_bayes_continuous.txt";
        Path path = new Path(uri);
        
		try{
	        FileSystem fs = FileSystem.get(URI.create( uri), context.getConfiguration());
	        if (fs.exists(path)) fs.delete(path, true);
            BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));
            br.write("class_id,     mu(mean),     std");
            br.write("-------------------------------\n");
            for (int i=0; i < number_of_classes; i++){
        		br.write("-------- Class-"+i+"-------\n");
            	for (int j=0; j < number_of_features; j++){
            		br.write(((FloatWritable)probablity_info_output.get(i)[j].get(new Text("class_id_mu")))+",  ");
            		br.write(((FloatWritable)probablity_info_output.get(i)[j].get(new Text("class_id_std")))+"\n");
            	}
            	br.write("\n");
            }
            br.close();
		}catch(Exception e){
            System.out.println("File /user/hduser/naive_bayes_continuous.txt cannot be found");
		}
		
	}
	
	@Override // necessary otherwise it runs default reduce()
	public void reduce(LongWritable key, Iterable<MapArrayWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		
		int key_index = (int) key.get();
		
        Float[] sigma_x2 = new Float[number_of_features];
        Float[] mu_x = new Float[number_of_features];
        Float[] num_x = new Float[number_of_features];
        Float[] partial_num_x = new Float[number_of_features];
        Float[] total_num_x = new Float[number_of_features];
        
        Float[] class_id_mu = new Float[number_of_features];
        Float[] class_id_std = new Float[number_of_features];


        MapWritable[] t =  new MapWritable[number_of_features];

        // It is a MUST to initilize all arrays before usage.
        for (int i=0; i<number_of_features;i++){ 
        	t[i] = new MapWritable(); // each member of an array (including MapWritable[] ) MUST be initilized before use
        	sigma_x2[i] = 0.0f;
            mu_x[i] = 0.0f;
            num_x[i] = 0.0f;
            partial_num_x[i] = 0.0f;
            total_num_x[i] = 0.0f;
            class_id_mu[i] = 0.0f;
            class_id_std[i] = 0.0f;
        }

       	for (MapArrayWritable val : values){
       		for (int i=0; i < number_of_features; i++){
				num_x[i] =  ((FloatWritable)((MapWritable) (val.get()[i])).get(new Text("num_x_local"))).get();
				sigma_x2[i] += ((FloatWritable)((MapWritable) (val.get()[i])).get(new Text("sigma_x2"))).get();
				mu_x[i] = ((FloatWritable)((MapWritable) (val.get()[i])).get(new Text("mu_x_local"))).get();

				partial_num_x[i] += mu_x[i]*num_x[i]; // calculates mu(i)*N(i)
				total_num_x[i] += num_x[i]; // calculates total N=N1+N2+...+Nk
       		}
       	}
        	
        for (int i=0; i<number_of_features & total_num_x[0]!=0 ; i++){
        	class_id_mu[i] = partial_num_x[i]/total_num_x[i];
        	class_id_std[i] = sigma_x2[i]/total_num_x[i]-(class_id_mu[i]*class_id_mu[i]);
        }

        for (int i=0; i<number_of_features & total_num_x[0]!=0; i++){
        	t[i].put(new Text("class_id_mu"), new FloatWritable(class_id_mu[i]));
        	t[i].put(new Text("class_id_std"), new FloatWritable(class_id_std[i]));
		}
       		
        	probablity_info_output.set(key_index, t);

	}
}
