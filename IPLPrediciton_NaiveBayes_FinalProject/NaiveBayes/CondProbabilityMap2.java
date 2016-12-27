package NaiveBayes;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CondProbabilityMap2 extends Mapper<LongWritable,Text,Text,LongWritable>{
		
		JobConf conf;
		public void configure(JobConf conf) {
		    this.conf = conf;
		}
		
		public void map(LongWritable key, Text value,Context context)throws IOException, InterruptedException{
			String line=value.toString();
			String[] input=line.split(",");
			try{
				context.write(new Text(input[6]), new LongWritable(1));
				context.write(new Text(input[3]+":"+input[6]), new LongWritable(1));
				context.write(new Text(input[4]+":"+input[6]), new LongWritable(1));
				context.write(new Text(input[5]+":"+input[6]), new LongWritable(1));
				
				if (input[1].equals("Deccan Chargers") || input[1].equals("Sunrisers Hyderabad")){
					context.write(new Text("Deccan Chargers:"+input[6]), new LongWritable(1));
					context.write(new Text("Sunrisers Hyderabad:"+input[6]), new LongWritable(1));
					
				}
				else if(input[1].equals("Pune Warriors") || input[1].equals("Rising Pune Supergiants")){
					context.write(new Text("Pune Warriors:"+input[6]), new LongWritable(1));
					context.write(new Text("Rising Pune Supergiants:"+input[6]), new LongWritable(1));
				}
				else{
					context.write(new Text(input[1]+":"+input[6]), new LongWritable(1));
				}
				

				if (input[2].equals("Deccan Chargers") || input[2].equals("Sunrisers Hyderabad")){
					context.write(new Text("Deccan Chargers:"+input[6]), new LongWritable(1));
					context.write(new Text("Sunrisers Hyderabad:"+input[6]), new LongWritable(1));
					
				}
				else if(input[2].equals("Pune Warriors") || input[2].equals("Rising Pune Supergiants")){
					context.write(new Text("Pune Warriors:"+input[6]), new LongWritable(1));
					context.write(new Text("Rising Pune Supergiants:"+input[6]), new LongWritable(1));
				}
				else{
					context.write(new Text(input[2]+":"+input[6]), new LongWritable(1));
				}
				
			}
			catch(Exception e){
				System.out.println("Exception : "+e);
			}
			
	
		}
	}	
	

