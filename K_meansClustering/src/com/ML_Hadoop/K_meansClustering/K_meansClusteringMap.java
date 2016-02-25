// *********************************************************************************************************
// *********************************************************************************************************
// Map function of K-means clustering in Hadoop
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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// The input of mappers is <LongWritable as key, Text as value> for reading text files line-by-line.
// The output of mappers is <LongWritable as key, FloatArrayWritable as value>
public class K_meansClusteringMap extends Mapper<LongWritable, Text, LongWritable, FloatArrayWritable>{

	private ArrayList<Float[]> cetroid_of_clusters = new ArrayList<Float[]>();
	private ArrayList<Float[]> sum_of_members_in_a_cluster = new ArrayList<Float[]>();
	private int[] num_of_members_in_a_cluster;

	int number_of_clusters, feature_size;
	
	@Override
	public void setup(Context context) {
		number_of_clusters = context.getConfiguration().getInt("number_of_clusters", 2);
		feature_size = context.getConfiguration().getInt("feature_size",1);
		num_of_members_in_a_cluster = new int[number_of_clusters];
		
		// initialization of ArrayLists 'cetroid_of_clusters', and 'sum_of_members_in_a_cluster' and array 'num_of_members_in_a_cluster'
		Float[] t = new Float[feature_size];
		for (int i=0; i< feature_size; i++)
			t[i]=0.0f;
		
		for (int i=0; i< number_of_clusters; i++){
			cetroid_of_clusters.add(t);
			sum_of_members_in_a_cluster.add(t);
			num_of_members_in_a_cluster[i]=0;
		}
		

		// Read the current values of cetroids of clusters from k_means.txt file
		// If it is the first iteration, the cetroids of clusters must be initialized as 
		// random number (regard to the min & max values of each features) or by user.
		
		try{
			Float[] t_float;
			String uri = "/user/hduser/k_mean.txt";
			FileSystem fs = FileSystem.get(context.getConfiguration());
			if (fs.exists(new Path(uri))){
				BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
				String[] temp;
				for (int i=0; i < number_of_clusters; i++){
					temp = br.readLine().split(",");
					t_float = new Float[feature_size];
					for (int j=0; j < feature_size; j++)
						t_float[j] = Float.parseFloat(temp[j]);
					cetroid_of_clusters.set(i, t_float);
				}
			} else {
				// initialization of clusters' centroids by user for our specific data. 
				// one good way is to randomly choose this values and put on "k_mean.txt" file as:
				//        cetroid of feature-1 seperated by ','
				//        cetroid of feature-2 seperated by ','
				// for example, regard to following values:
				//        13.325872,16.854961
				//        13.5158205,8.382423
				//        16.05023,4.76127
				t_float = new Float[2];
			    t_float[0]=13.325872f;
			    t_float[1]=16.854961f;
			    cetroid_of_clusters.set(0, t_float);

			    t_float = new Float[2];
			    t_float[0]=13.5158205f;
			    t_float[1]=8.382423f;
				cetroid_of_clusters.set(1, t_float);

				t_float = new Float[2];
			    t_float[0]=16.05023f;
			    t_float[1]=4.76127f;
				cetroid_of_clusters.set(2, t_float);

			}
		}catch (Exception e){
			
		} 
	} 
	
	// cleanup function is used as innermap to aggregate data before shuffling
	// The output of mappers is FloatArrayWritable with size of 'feature_size+1'.
	// The first value in this FloatArrayWritable is # of members in a cluster. Then
	// the rest of FloatArrayWritable is the summation of all members in a cluster. 
	@Override 
	protected void cleanup(Context context) throws IOException, InterruptedException{
		FloatWritable[] temp = new FloatWritable[feature_size+1];
		
		for (int i=0; i < number_of_clusters; i++){
			temp[0] = new FloatWritable(num_of_members_in_a_cluster[i]);
			for (int j=1; j < feature_size+1; j++){
				temp[j] = new FloatWritable(sum_of_members_in_a_cluster.get(i)[j-1]);
			}
			context.write(new LongWritable(i), new FloatArrayWritable(temp));
		}
	}
	
	@Override // the word '@Override' is necessary. otherwise it runs defaults map()
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// Read each line of input file and extract each value of data  
		String value_of_a_line_of_a_input_file = value.toString(); 
		String[] split_data_of_the_line = value_of_a_line_of_a_input_file.split("\t"); // split line by TAB
		Float[] values_of_data_of_the_line = new Float[feature_size]; 
		
		for (int i=0; i < feature_size; i++)
			values_of_data_of_the_line[i] = Float.parseFloat(split_data_of_the_line[i]);
		
		similarityCheck(values_of_data_of_the_line);
		
	}
	
    // This function computes the distance between each data and the centroids of each cluster.
	// Then labels the data with cluster's label with the lowest distance. Also, keeps how many
	// of data has been labeled for different clusters.
	private void similarityCheck(Float[] values){
		Float[] temp = new Float[number_of_clusters];
		
		for (int i=0; i < number_of_clusters; i++)
			temp[i] = Compute_Distance(values, i);

		// find the lowest distance between the data and centroids' of clusters and label data
		Float t = temp[0]; 
		int cluster_index=0;
		for (int i=1; i < number_of_clusters; i++)
			if (temp[i] < t){
				cluster_index = i;
				t = temp[i];
			}
		num_of_members_in_a_cluster[cluster_index]++;
		
		temp = new Float[feature_size];
		for (int i=0; i< feature_size; i++)
			temp[i] = sum_of_members_in_a_cluster.get(cluster_index)[i] + values[i];
		sum_of_members_in_a_cluster.set(cluster_index, temp);
		//return index;
	}
	
	private Float Compute_Distance(Float[] values, int cluster_index){
		Float temp=0.0f;
		for (int i=0; i<feature_size; i++){
			// compute Euclidean distance. As it is only used for comparison, Math.sqrt() is not necessary.
			temp += (Float) (values[i]-(cetroid_of_clusters.get(cluster_index))[i])*(values[i]-(cetroid_of_clusters.get(cluster_index))[i]);
		}
		return temp; 
		
	}
}
