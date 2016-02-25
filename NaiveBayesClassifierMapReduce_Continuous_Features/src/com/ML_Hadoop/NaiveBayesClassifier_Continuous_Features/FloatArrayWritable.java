package com.ML_Hadoop.NaiveBayesClassifier_Continuous_Features;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

/**
 * Must subclass ArrayWritable if it is to be the input to a Reduce function
 * because the valueClass is not written to the output. Wish there was
 * some documentation which said that...
 * 
 * @author Daniel
 *
 */
public class FloatArrayWritable extends ArrayWritable {

	public FloatArrayWritable(Writable[] values) {
		super(FloatWritable.class, values);
	}

	public FloatArrayWritable() {
		super(FloatWritable.class);
	}

	public FloatArrayWritable(Class valueClass, Writable[] values) {
		super(FloatWritable.class, values);
	}

	public FloatArrayWritable(Class valueClass) {
		super(FloatWritable.class);
	}

	public FloatArrayWritable(String[] strings) {
		super(strings);
	}

}

/*
 * Copyright Â© 2008 California Polytechnic State University
 * Licensed under the Creative Commons 
 * Attribution 3.0 License -http://creativecommons.org/licenses/by/3.0/
 */
