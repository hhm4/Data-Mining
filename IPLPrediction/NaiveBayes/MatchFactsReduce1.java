package NaiveBayes;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MatchFactsReduce1 extends Reducer<Text,FloatWritable,Text,NullWritable>{
	public void reduce(Text key,Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException{
		context.write(key, NullWritable.get());
	}
	
}	

