import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class OnSchedule {

		
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
    	Job job = new Job(conf, "OnSchedule");
    	job.setJarByClass(OnSchedule.class);
    	job.setMapperClass(OnScheduleMappr.class);
    	job.setReducerClass(OnScheduleReducr.class);
    	
    	job.setMapOutputKeyClass(Text.class);
    	job.setMapOutputValueClass(Text.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(Text.class);
    	
    	FileInputFormat.setInputPaths(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
   	 	job.waitForCompletion(true);
	}
}
class OnScheduleMappr extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
		String textline = value.toString();
		String[] linevalues = textline.split(",");
		if(textParse(linevalues[14])>10)  //assuming that if the delay is less than 10 then then there is no delay
			context.write(new Text(linevalues[8]), new Text("1"));
		else
			context.write(new Text(linevalues[8]), new Text("0"));
	}

	int textParse(String text){
		try{
			int parsedString = Integer.parseInt(text);
			return parsedString;
		}
		catch(NumberFormatException ex){
			return 0;
		}
	}
}

class OnScheduleReducr extends Reducer<Text, Text, Text, Text> {
	TreeSet<Pair> pairList = new TreeSet<Pair>();
	public void reduce(Text key, Iterable<Text> values, Context context) {
		int onTime_Count = 0;
		int delayCount = 0;

		for(Text val: values){
			if(Integer.parseInt(val.toString()) == 0)
				onTime_Count+=1;
			else if(Integer.parseInt(val.toString()) == 1)
				delayCount+=1;
		}

		double prob = (double)onTime_Count / (double)(onTime_Count+delayCount);
		if(prob!=1.00){
		pairList.add(new Pair(key.toString(), prob));
		}
	}


	
	class Pair implements Comparable<Pair>{
		String CarrierName;
		double probablity;


		public Pair(String CarrierName, double probablity) {
			this.CarrierName = CarrierName;
			this.probablity = probablity;
		}

		@Override
		public int compareTo(Pair pair) {
			if (this.probablity > pair.probablity) 
				return 1;
			else if(this.probablity<pair.probablity)
				return -1;
			else
				return 0;
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text("The 3 airlines with the highest probability:"), null);
		context.write(new Text("Carrier Name"+"\t"+"probibility"), null);
		for (int i=0; i<3; i++) {
			Pair last = pairList.pollLast();
			context.write(new Text(last.CarrierName+"\t"), new Text(Double.toString(last.probablity)));
		}

		context.write(new Text("The 3 airlines with the lowest probability:"), null);
		context.write(new Text("Carrier Name"+"\t"+"probibility"), null);
		for(int i=0;i<3;i++){
			Pair first = pairList.pollFirst();
			context.write(new Text(first.CarrierName+"\t"), new Text(Double.toString(first.probablity)));
		}
	}

}