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

public class FlightCancellation{

	
	public static void main(String[] args) throws Exception {
				
		Configuration conf = new Configuration();
    	Job job = new Job(conf, "FlightCancellation");
    	job.setJarByClass(FlightCancellation.class);
    	job.setMapperClass(FlightCancellationMapper.class);
    	job.setReducerClass(FlightCancellationReducer.class);
    	
    	job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(Text.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);
    	
    	FileInputFormat.setInputPaths(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
   	 	job.waitForCompletion(true);
	}
}


class FlightCancellationMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
		int count = 0;
		String textlines = value.toString();
		String[] colValues = textlines.split(",");
		//Retreiving Cancelled column
		if(textParse(colValues[21])==1){
			//Retriving Cancelled Reason of flight
			context.write(new Text(colValues[22]), new Text("1"));
		}
	}
	int textParse(String text){
		try{
			int parsedText = Integer.parseInt(text);
			return parsedText;
		}
		catch(NumberFormatException ex){
			return 0;
		}
	}
}

class FlightCancellationReducer extends Reducer<Text, Text, Text, Text> {
	TreeSet<Pair> FlightList = new TreeSet<Pair>();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int count = 0;
		for(Text value: values){
			count += Integer.parseInt(value.toString());
		}
		FlightList.add(new Pair(key.toString(), count));
	}

	
	class Pair implements Comparable<Pair>{
		String canreason;
		int count;

		public Pair(String reason, int count) {
			this.canreason = reason;
			this.count = count;
		}

		@Override
		public int compareTo(Pair pair) {
			if (this.count > pair.count) 
				return 1;
			else if (this.count < pair.count)
				return -1;
			else
				return 0;
		}
	}
	protected void cleanup(Context context) throws IOException, InterruptedException {
		String cancel_reason = "";

		Pair pair = FlightList.pollLast();
		if(pair.canreason.equals("A")){
			cancel_reason = "Carrier Delay";
		}else if(pair.canreason.equals("B")){
			cancel_reason = "Weather Delay";
		}else if(pair.canreason.equals("C")){
			cancel_reason = "NAS Delay";
		}else if(pair.canreason.equals("D")){
			cancel_reason = "Security Delay";
		}else{
			cancel_reason = "No Reason";
		}
		
		context.write(new Text("Flight Cancellation Reason : "), new Text(cancel_reason));
		
	}
}