package com.ML_Hadoop.NaiveBayesClassifier_Continuous_Features;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;

/**
 * Must subclass ArrayWritable if it is to be the input to a Reduce function
 * because the valueClass is not written to the output. Wish there was
 * some documentation which said that...
 * 
 * @author Nikzad Babaii Rizvandi
 *
 */
public class MapArrayWritable extends ArrayWritable {

	public MapArrayWritable(MapWritable[] values) {
		super(MapWritable.class, values);
	}

	public MapArrayWritable() {
		super(MapWritable.class);
	}

	public MapArrayWritable(Class valueClass, MapWritable[] values) {
		super(MapWritable.class, values);
	}

	public MapArrayWritable(Class valueClass) {
		super(MapWritable.class);
	}

	public MapArrayWritable(String[] strings) {
		super(strings);
	}

}
