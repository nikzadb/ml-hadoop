// *********************************************************************************************************
// *********************************************************************************************************
// Reduce function of K-means clustering in Hadoop
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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import com.ML_Hadoop.K_meansClustering.FloatArrayWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

// The input of reducers is <LongWritable as key, FloatArrayWritable as value> which must be compatible by mapper output.
// The output of reducers is <LongWritable as key, FloatArrayWritable as value>. 
// This Map-Reduce job is iterative. However, the official output of reducers are not used as input for mappers on next
// iteration. Reducers update another file, i.e., "/user/hduser/k_mean.txt", which is used by mappers on next iteration.

public class K_meansClusteringReduce extends Reducer<LongWritable, FloatArrayWritable, LongWritable, FloatArrayWritable> {
	Configuration conf;
	private int current_iteration_num, feature_size, number_of_clusters;
	private ArrayList<Float[]> cetroid_of_a_cluster;
	private ArrayList<Float[]> sum_of_members_of_a_cluster;
	private ArrayList<FloatArrayWritable> cetroids_of_all_clusters = new ArrayList<FloatArrayWritable>();

	 
	@Override
    public void setup(Context context) {
		current_iteration_num = context.getConfiguration().getInt("current_iteration_num",1);
		feature_size = context.getConfiguration().getInt("feature_size",1);
		number_of_clusters = context.getConfiguration().getInt("number_of_clusters",1);
		cetroid_of_a_cluster = new ArrayList<Float[]>();
		sum_of_members_of_a_cluster = new ArrayList<Float[]>();
		Float[] temp = new Float[feature_size];
		
		// initialization of ArrayLists 'cetroid_of_a_cluster' and 'sum_of_members_of_a_cluster'.
		for (int i=0; i< feature_size; i++){
			temp[i]=0.0f;
		}

		for (int i=0; i< number_of_clusters; i++){
			cetroid_of_a_cluster.add(temp);
			sum_of_members_of_a_cluster.add(temp);
		}

	}
	
	@Override
	protected void cleanup(Context context) throws IOException{
		String uri = "/user/hduser/k_mean.txt";
        Path path = new Path( uri);
		
        // Write the latest values of cetroids' of clusters in 'k_mean.txt' file
		try{
	        FileSystem fs = FileSystem.get(URI.create( uri), context.getConfiguration());
	        if (fs.exists(path)) fs.delete(path, true);
            BufferedWriter br=new BufferedWriter(new OutputStreamWriter(fs.create(path,true)));
            for (int i=0; i < number_of_clusters; i++){
            	for (int j=0; j<feature_size; j++)
            		br.write(((Float) ((FloatWritable)cetroids_of_all_clusters.get(i).get()[j]).get()).toString()+",");
            	br.write("\n");
            }
            br.close();
		}catch(Exception e){
            System.out.println("File k_mean.txt not found");
		}
		
        // Write the values of cetroids' of clusters for current iteration in directory '/user/hduser/K-means/...'

		uri = "/user/hduser/K-means/means-"+current_iteration_num+".txt";
        path = new Path(uri);
		
		try{
	        FileSystem fs = FileSystem.get(context.getConfiguration());
	        if (current_iteration_num == 0) 
	        	fs.delete(new Path("/user/hduser/K-means"), true);
	        OutputStreamWriter osw = new OutputStreamWriter(fs.create(path,true));
            BufferedWriter br=new BufferedWriter(osw);
            for (int i=0; i < number_of_clusters; i++){
            	for (int j=0; j<feature_size; j++)
            		br.write((Float) ((FloatWritable)cetroids_of_all_clusters.get(i).get()[j]).get()+",");
            	br.write("\n");
            }
            br.close();
		}catch(Exception e){
            System.out.println("File not found");
		}
	}
	
	@Override // The word '@Override' is necessary otherwise it runs default reduce()
	public void reduce(LongWritable key, Iterable<FloatArrayWritable> values, Context context) throws IOException, InterruptedException {

		int[] num_of_members_in_a_cluster = new int[number_of_clusters]; 
		int key_index = (int) key.get();
		
   		for (FloatArrayWritable val : values){
   			num_of_members_in_a_cluster[key_index] += (int) ((FloatWritable) (val.get())[0]).get();
   			Float[] temp = new Float[feature_size];
   			for (int i=0; i<feature_size; i++){
   				temp[i] = sum_of_members_of_a_cluster.get(key_index)[i] + (Float) ((FloatWritable) (val.get())[i+1]).get();
   			}
   			sum_of_members_of_a_cluster.set(key_index, temp);
   		}

		Float[] temp = new Float[feature_size];
   		for (int i=0; i<feature_size; i++){
   			temp[i] = sum_of_members_of_a_cluster.get(key_index)[i]/num_of_members_in_a_cluster[key_index];
   		}
   		cetroid_of_a_cluster.set(key_index, temp);
   		
   	    FloatWritable[] t = new FloatWritable[feature_size];
		for (int i=0; i< feature_size; i++){
			t[i] = new FloatWritable(cetroid_of_a_cluster.get(key_index)[i]);
		}
  	
   		cetroids_of_all_clusters.add(new FloatArrayWritable(t));
		context.write(key, new FloatArrayWritable(t));
    	
      }
      
      
}