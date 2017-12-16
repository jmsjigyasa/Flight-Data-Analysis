import java.io.IOException;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CancellationReason {
	
			public static TreeSet<ResultPair> cancelReasonCount = new TreeSet<>();
			public static class CancelReasonMapper extends Mapper<Object, Text, Text, IntWritable>
			{
			
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException
			{
				String line = value.toString();
				String[] fields = line.split(",");
				String cancellationCode = fields[22];
				String cancellationReason="no reason given";
				String cancelled=fields[21];
			//  reason for cancellation (A = carrier, B = weather, C = NAS, D = security)
				if(cancelled.equals("1")&&!cancellationCode.equals("NA"))
				//if((cancellationCode.equalsIgnoreCase("A") || cancellationCode.equalsIgnoreCase("B")|| cancellationCode.equalsIgnoreCase("C")|| cancellationCode.equalsIgnoreCase("D") ))
		     	{ 


					switch(cancellationCode){
						case "A" : cancellationReason = "carrier";
									break;
						case "B" : cancellationReason = "weather";
									break;
						case "C" : cancellationReason = "NAS";
									break;
						case "D" : cancellationReason = "security";
									break;

						

					}

					context.write(new Text(cancellationReason), new IntWritable(1));
					
				}

				//if(cancelled.equals("1")&&cancellationCode.equals("NA"))
				//context.write(new Text(cancellationReason), new IntWritable(1));
			
			}
		
			}
	
		
			public static class CancelReasonReducer extends Reducer<Text, IntWritable, Text, IntWritable>
			{
		
			public void reduce(Text key, Iterable<IntWritable> values , Context context) throws IOException, InterruptedException 
			{
				int total = 0;
				for(IntWritable val : values)
					{
					total = total + val.get();
					}
				//adding to tree set
				cancelReasonCount.add( new ResultPair(key.toString(), total) );
			
				// We need only top 1 reason , so need to keep storing less values
				if(cancelReasonCount.size() > 1)
				{
					cancelReasonCount.pollLast();
				}
			
			
			}
		
		 protected void cleanup(Context context) throws IOException, InterruptedException 
			 {

			 context.write(new Text("Top Reason for Cancellation:  "),null);
    		  while (!cancelReasonCount.isEmpty())
				  {
					ResultPair topReason = cancelReasonCount.pollFirst();
					
					context.write(new Text(topReason.reason), new IntWritable(topReason.count));
				 }
			}
			
			}//end of Reducer class



		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
			{
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(CancellationReason.class);

		job.setMapperClass(CancelReasonMapper.class);
		job.setReducerClass(CancelReasonReducer.class);
		

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
		
		}

		public static class ResultPair implements Comparable<ResultPair>
		{
		
		String reason ;
		int count;
		ResultPair(String reason , int count)
			{
			this.reason  =  reason ;
			this.count = count;
			}

		public int compareTo(ResultPair resultPair) 
			{
			if(this.count <= resultPair.count)
				return 1;
			else
				return -1;
			}
		}

}

