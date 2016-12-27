package NaiveBayes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class IPLPrediction {
	
	
	public static void main(String []args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException{
		
		Configuration conf=new Configuration();
		Job matchFacts=new Job(conf,"Rewrite Records");
		matchFacts.setJarByClass(IPLPrediction.class);    
		DistributedCache.addCacheFile (new Path("/refer/HomeGrounds.txt").toUri(), matchFacts.getConfiguration());
	//	FileInputFormat.addInputPath(wordCount, new Path(args[0]));//input1
		//FileOutputFormat.setOutputPath(wordCount,new Path(args[1]));//output1 & input2
		FileInputFormat.addInputPath(matchFacts, new Path("/trainDataSet"+args[0]));
		FileOutputFormat.setOutputPath(matchFacts,new Path("/matchFacts"));
		matchFacts.setMapperClass(MatchFactsMap1.class);
		matchFacts.setReducerClass(MatchFactsReduce1.class);
		matchFacts.setMapOutputKeyClass(Text.class);
		matchFacts.setMapOutputValueClass(NullWritable.class);
		matchFacts.setOutputKeyClass(Text.class);
		matchFacts.setOutputValueClass(NullWritable.class);
		matchFacts.waitForCompletion(true);
		
		conf.set("mapred.textoutputformat.separator", ",");
		Job condProbability=new Job(conf,"Find Probability");
		condProbability.setJarByClass(IPLPrediction.class); 
		//FileInputFormat.addInputPath(wordCount, new Path(args[0]));//input1
		//FileOutputFormat.setOutputPath(wordCount,new Path(args[1]));//output1 & input2
		LazyOutputFormat.setOutputFormatClass(condProbability, TextOutputFormat.class);
		MultipleOutputs.addNamedOutput(condProbability, "text", TextOutputFormat.class,Text.class, Text.class);
		FileInputFormat.addInputPath(condProbability, new Path("/matchFacts"));
		FileOutputFormat.setOutputPath(condProbability,new Path("/probability"));
		condProbability.setMapperClass(CondProbabilityMap2.class);
		condProbability.setReducerClass(CondProbabilityReduce2.class);
		condProbability.setMapOutputKeyClass(Text.class);
		condProbability.setMapOutputValueClass(LongWritable.class);
		condProbability.setOutputKeyClass(Text.class);
		condProbability.setOutputValueClass(Text.class);
		condProbability.waitForCompletion(true);
		
		Job predict=new Job(conf,"Make Prediction");
		predict.setJarByClass(IPLPrediction.class);    
		DistributedCache.addCacheFile (new Path("/refer/HomeGrounds.txt").toUri(), matchFacts.getConfiguration());
		DistributedCache.addCacheFile (new Path("/probability/condprob-r-00000").toUri(), predict.getConfiguration());
		//	FileInputFormat.addInputPath(wordCount, new Path(args[0]));//input1
		//FileOutputFormat.setOutputPath(wordCount,new Path(args[1]));//output1 & input2
		FileInputFormat.addInputPath(predict, new Path("/testDataSet"+args[0]));
		FileOutputFormat.setOutputPath(predict,new Path("/prediction"));
		predict.setMapperClass(PredictMap3.class);
		predict.setReducerClass(PredictReduce3.class);
		predict.setMapOutputKeyClass(Text.class);
		predict.setMapOutputValueClass(Text.class);
		predict.setOutputKeyClass(Text.class);
		predict.setOutputValueClass(Text.class);
		predict.waitForCompletion(true);
		
		
		Job validate=new Job(conf,"Validate Predictions");
		validate.setJarByClass(IPLPrediction.class);    
		//	FileInputFormat.addInputPath(wordCount, new Path(args[0]));//input1
		//FileOutputFormat.setOutputPath(wordCount,new Path(args[1]));//output1 & input2
		FileInputFormat.addInputPath(validate, new Path("/prediction"));
		FileOutputFormat.setOutputPath(validate,new Path("/validation"+args[0]));
		validate.setMapperClass(ValidateMap4.class);
		validate.setReducerClass(ValidateReduce4.class);
		validate.setMapOutputKeyClass(Text.class);
		validate.setMapOutputValueClass(LongWritable.class);
		validate.setOutputKeyClass(Text.class);
		validate.setOutputValueClass(LongWritable.class);
		validate.waitForCompletion(true);
		
		
		System.exit(condProbability.waitForCompletion(true) ? 0 : 1);
		
	}
}

