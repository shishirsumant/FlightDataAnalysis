import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AVGTaxiTime {

	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
    	Job job = new Job(conf, "AVGTaxiTime");
    	job.setJarByClass(AVGTaxiTime.class);
    	job.setMapperClass(AVGTaxiTimeMapper.class);
    	job.setReducerClass(AVGTaxiTimeReducer.class);
    	
    	job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(Text.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);
    	
    	FileInputFormat.setInputPaths(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
   	 	job.waitForCompletion(true);
   	 	
			}
}

class AVGTaxiTimeMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
		String textLines = value.toString();
		String[] colValues = textLines.split(",");
		
		//Retrieving TaxiOut Column value from CSV and parsing String to Integer
		if(textParse(colValues[20])){
			//Retrieving origin Airport code and Taxi out Time
			context.write(new Text(colValues[16]), new Text(colValues[20]));
		}
		//Retrieving TaxiIn Column value from CSV and parsing String to Integer
		if(textParse(colValues[19])){
			//Retrieving Destination Airport code and TaxiIn time
			context.write(new Text(colValues[17]), new Text(colValues[19]));
		}
	}
	boolean textParse(String inputText){
		try{
			//Parsing String to Integer
			int parsedText = Integer.parseInt(inputText);
			return true;
		}
		catch(NumberFormatException ex){
			return false;
		}
	}
}


class AVGTaxiTimeReducer extends Reducer<Text, Text, Text, Text> {
	TreeSet<Pair> TaxiList = new TreeSet<Pair>();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		int taxiTime = 0;
		for(Text val: values){
			count+=1;
			taxiTime+=Integer.parseInt(val.toString());	
		}
		double avg = (double)taxiTime / (double) count;
		TaxiList.add(new Pair(key.toString(),avg));
	}

	
	class Pair implements Comparable<Pair>{
		String airportCode;
		double taxi_Time;

		public Pair(String airPortName, double taxiTime) {
			this.airportCode = airPortName;
			this.taxi_Time = taxiTime;
		}

		
		public int compareTo(Pair pair) {
			if (this.taxi_Time > pair.taxi_Time) 
				return 1;
			else if(this.taxi_Time < pair.taxi_Time)
				return -1;
			else
				return 0;
		}
	}
	
protected void cleanup(Context context) throws IOException, InterruptedException {
		
		if(TaxiList.size()==0){
			context.write(new Text("No TaxiIn and TaxiOut for all the Airports in Current Year"), null);
		}else{
		context.write(new Text("The 3 airports with the longest taxi time are:"), null);
		context.write(new Text("Airport Code"+"\t"+"Taxi Time"), null);
		for(int i=0;i<3;i++){
			Pair lastValue = TaxiList.pollLast();
			context.write(new Text(lastValue.airportCode), new Text(Double.toString(lastValue.taxi_Time)));
		}

		context.write(new Text("The 3 airports with the shortest taxi time are:"), null);
		context.write(new Text("Airport Code"+"\t"+"Taxi Time"), null);
		for(int i=0;i<3;i++){
			Pair firstValue = TaxiList.pollFirst();
			context.write(new Text(firstValue.airportCode), new Text(Double.toString(firstValue.taxi_Time)));
		}
		}
	}
}