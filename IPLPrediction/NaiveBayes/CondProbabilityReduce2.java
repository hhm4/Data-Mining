package NaiveBayes;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

	public class CondProbabilityReduce2 extends Reducer<Text,LongWritable,Text,Text>{
		private MultipleOutputs<Text, Text> multipleOutputs;
		
		public void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException{
			
			int count=0;
			for(LongWritable val:values){
				count=count+1;
			}
			
		//	context.write(key,new Text(String.valueOf(count)));
			multipleOutputs.write(key,new Text(String.valueOf(count)), "condprob");
			
		}
		
		@Override
		public void setup(Context context){
			multipleOutputs = new MultipleOutputs<Text, Text>(context);
		}
		
		@Override
		public void cleanup(final Context context) throws IOException, InterruptedException{
			multipleOutputs.close();
		}
		
		
	}	
	

