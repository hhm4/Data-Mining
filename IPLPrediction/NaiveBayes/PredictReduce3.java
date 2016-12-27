package NaiveBayes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class PredictReduce3 extends Reducer<Text,Text,Text,Text>{
		
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			Map<String,Float> results=new HashMap<String,Float>();
			String team1=null;
			String team2=null;
			String winner=null;
			for(Text val:values){
				String[] value=val.toString().split(":");
				if (value[0].equals("*")){
					team1=value[1];
					team2=value[2];
					try{
						winner=value[3];
					}
					catch(Exception e){
						System.out.println("Exception"+e);
					}
				}
				else{
					results.put(value[0]+":"+value[1], Float.valueOf(value[2]));
				}
			}
			String team=null;
			String result=null;
			for (Entry<String,Float>e:entriesSortedByValues(results)){
	        	team=e.getKey().split(":")[0];
	        	result=e.getKey().split(":")[1];
	        	break;
	        }
			if (result.equals("won")){
				context.write(new Text(key.toString()+":"+team1+":vs:"+team2+",WonBy:"+winner), new Text("Prediction:"+team));
				
			}
			else{
				if (team1.equals(team)){
					context.write(new Text(key.toString()+":"+team1+":"+team2+",WonBy:"+winner), new Text("Prediction:"+team2));
				}
				else{
					context.write(new Text(key.toString()+":"+team1+":"+team2+",WonBy:"+winner), new Text("Prediction:"+team1));
				}
			}
			
		}
		
		
		
		static <K,V extends Comparable<? super V>>  List<Entry<K, V>> entriesSortedByValues(Map<K,V> map) {
			
			List<Entry<K,V>> sortedEntries = new ArrayList<Entry<K,V>>(map.entrySet());
			Collections.sort(sortedEntries, new Comparator<Entry<K,V>>() {
			        @Override
			        public int compare(Entry<K,V> e1, Entry<K,V> e2) {
			            return e2.getValue().compareTo(e1.getValue());
			        }
			    }
			);
			return sortedEntries;
		}
		
	}
	
	class ValueComparator implements Comparator<String> {
	    Map<String, Float> base;

	    public ValueComparator(Map<String, Float> base) {
	        this.base = base;
	    }

	    // Note: this comparator imposes orderings that are inconsistent with
	    // equals.
	    public int compare(String a, String b) {
	        if (base.get(a) >= base.get(b)) {
	            return -1;
	        } else {
	            return 1;
	        } // returning 0 would merge keys
	    }
	}	
	

