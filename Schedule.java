import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//calulate probality being on schedule = 1- No of times delayed/ total numbes of times it flew in that year-- larger the value - higher chance of being on time
//Note -some values col //15  //16 are negative

public class Schedule{

	public static TreeSet<ResultPair> Carrier_HighOnSchedule = new TreeSet<>();
	public static TreeSet<ResultPair> Carrier_LowOnSchedule = new TreeSet<>();
	
	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
			{
			String values[] = value.toString().split(",");
			String carrier = values[8];//UniqueCarrier unique carrier code(airline)
			
			//15 ArrDelay arrival delay, in minutes
			//16 DepDelay departure delay, in minutes
			// We could have used CarrierDelay but that column all NA

			if (isInteger(values[14])) {
				if (Integer.parseInt(values[14]) >= 10) 
				{
					Text delayedCarrier = new Text(carrier);
					context.write(delayedCarrier, new LongWritable(1));
				}
				Text countTotFlights = new Text(carrier+"_CountFlts");
				context.write(countTotFlights, new LongWritable(1));				

			}

			if (isInteger(values[15])) {
				if (Integer.parseInt(values[15]) >= 10) 
				{
					Text delayedCarrier = new Text(carrier);
					context.write(delayedCarrier, new LongWritable(0));
				}
				//Count the total flights of that airline in that year/file
				Text countTotFlights = new Text(carrier+"_CountFlts");
				context.write(countTotFlights, new LongWritable(1));
			}
			
			//For flights having 0 or -ve arrival/dep delay we put zero

			if (isInteger(values[14]))
			{
				if (Integer.parseInt(values[14]) >= 10) 
				{
					Text delayedCarrier = new Text(carrier);
					context.write(delayedCarrier, new LongWritable(0));
				}
				Text countTotFlights = new Text(carrier+"_CountFlts");
				context.write(countTotFlights, new LongWritable(0));
			}
			
			if (isInteger(values[15])) 
			{
				if (Integer.parseInt(values[15])>= 10)
				{
					Text delayedCarrier = new Text(carrier);
					context.write(delayedCarrier, new LongWritable(0));
				}
				//Count the total flights of that airline in that year/file
				Text countTotFlights = new Text(carrier+"_CountFlts");
				context.write(countTotFlights, new LongWritable(0));
			}


		}// end of Mapper function
		
		public static boolean isInteger(String s) {
			boolean isValidInteger = false;
			try {
				Integer.parseInt(s);
				isValidInteger = true;
			} catch (NumberFormatException ex)
			{
			}
			return isValidInteger;
		}
	}// end of MapperClass 

	public static class Combiner extends Reducer<Text, LongWritable, Text, LongWritable> {

		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
			{

			long count = 0;
			for (LongWritable val : values)
				{
				count =count+ val.get();
				}
			context.write(key, new LongWritable(count));
			}

	}//end of combiner class
	
	public static class Reduce extends Reducer<Text, LongWritable, Text, DoubleWritable> {
		private DoubleWritable DelayedFlt = new DoubleWritable();
		private DoubleWritable totalTimesFlt = new DoubleWritable();
		private Text currentKey = new Text("Dummy");
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
			{
			//String airline = key.toString();
			if(key.toString().equalsIgnoreCase(currentKey.toString()+"_CountFlts")) // this check to ensure numerator and denominator are of same carrier
				{

				totalTimesFlt.set(getTotalCount(values));
				
				double ProbOfDelay =1-(DelayedFlt.get()/totalTimesFlt.get());
				Carrier_HighOnSchedule.add(new ResultPair(ProbOfDelay, currentKey.toString()));
				Carrier_LowOnSchedule.add(new ResultPair(ProbOfDelay, currentKey.toString()));
				
				//Tree is descending, hence top 3 are high and bottom 3 are low on schedule
				if(Carrier_HighOnSchedule.size()>3)
					{
					Carrier_HighOnSchedule.pollLast();
					}
				if(Carrier_LowOnSchedule.size()>3)
					{
					Carrier_LowOnSchedule.pollFirst();
					}

				
				
			}
			else{
				currentKey.set(key.toString());
				DelayedFlt.set(getTotalCount(values));
				}

				


			
		}// end of reduce function
		
		private double getTotalCount(Iterable<LongWritable> values) {
			double count = 0;
			for (LongWritable value : values) {
				count += value.get();
				}
			return count;
		}
		
		// reducer function gave proablities high and low same key or same flight number. Hence use cleanup to get overall comparison between all keys
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text("Highest on schedule probability" ), null);
			 while (!Carrier_HighOnSchedule.isEmpty()) 
				 {
	    		  ResultPair resultPair = Carrier_HighOnSchedule.pollFirst();
	              context.write(new Text(resultPair.carrier), new DoubleWritable(resultPair.probabilityOnSchedule));
				  }
			 context.write(new Text("Lowest on schedule probability" ), null );
	    	  while (!Carrier_LowOnSchedule.isEmpty())
				  {
	    		  ResultPair resultPair = Carrier_LowOnSchedule.pollLast();
	              context.write(new Text(resultPair.carrier), new DoubleWritable(resultPair.probabilityOnSchedule));
	             }
	    	 
	    	  
			}
		
	}//end of reducer class

	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(Schedule.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Combiner.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.waitForCompletion(true);
		
	}
	
	public static class ResultPair implements Comparable<ResultPair> {
		double probabilityOnSchedule;
		String carrier;
		

		ResultPair(double probabilityOnSchedule, String carrier)
			{
			this.probabilityOnSchedule = probabilityOnSchedule;
			this.carrier = carrier;
			}

		@Override
		public int compareTo(ResultPair resultPair) {
			if (this.probabilityOnSchedule <= resultPair.probabilityOnSchedule)
				{
				return 1;
				} 
				else 
					{
				return -1;
				}
			
		}//end of compare
	}// end of ResultPair


}// end the class