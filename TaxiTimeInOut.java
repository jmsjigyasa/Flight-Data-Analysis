import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TaxiTimeInOut
	{
	
	public static TreeSet<ResultPair> Airports_HighAvgTaxiTime = new TreeSet<>();
	public static TreeSet<ResultPair> Airports_LowAvgTaxiTime = new TreeSet<>();
	
	public static class TaxiInOutMap extends Mapper<Object, Text, Text, Text>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
			{
			String line = value.toString();
			String[] fields = line.split(",");
			
			
			String originAirport = fields[16]; // origin IATA airport code
			String destinationAirport = fields[17]; // destination IATA airport code
			String taxiIn = fields[19]; //tax in column in min for destination  
			String taxiOut = fields[20]; //taxi out column in min for orgin 
			if(!(taxiOut.equalsIgnoreCase("NA") || taxiOut.equalsIgnoreCase("TaxiOut") || originAirport.equalsIgnoreCase("Origin") ))
				{
				 	context.write(new Text(originAirport), new Text(taxiOut));
			    }
			  
			if(!(taxiIn.equalsIgnoreCase("NA") || taxiIn.equalsIgnoreCase("TaxiIn") || destinationAirport.equalsIgnoreCase("Dest") ))
				{
				    context.write(new Text(destinationAirport), new Text(taxiIn));
     			}
			 
		   }
	}
	
	public static class TaxiInOutReduce extends Reducer<Text, Text, Text, DoubleWritable>{


		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException
			{
			
			int total =0;
			double sum = 0;
			// Adding Taxi In and Out Values of the airport
			for(Text val : values)
				{
				total=total+1;
				sum =sum + Double.parseDouble(val.toString());
				}
			
			//Taking the average  taxi time for the airport
			double avgTaxiTime = sum / total;
			//double avgTaxiTime = 1.0;
			ResultPair airportTaxiTime = new ResultPair(key.toString(), avgTaxiTime);
			Airports_HighAvgTaxiTime.add(airportTaxiTime);
			Airports_LowAvgTaxiTime.add(airportTaxiTime);
			
			//Tree is descending, hence top 3 are high and bottom 3 are low on avg tzxi timezzzzz
			if(Airports_HighAvgTaxiTime.size()>3)
				{
				Airports_HighAvgTaxiTime.pollLast();
				}
			
			if(Airports_LowAvgTaxiTime.size()>3)
				{
				Airports_LowAvgTaxiTime.pollFirst();
				}
			
		}
		
		 protected void cleanup(Context context) throws IOException, InterruptedException 
			 {
				
			//Tree is descending, hence top 3 are high and bottom 3 are low on schedule	
			 context.write(new Text("Airport with Highest Average TaxiTime"), null);
			 while (!Airports_HighAvgTaxiTime.isEmpty())
				 {
				   ResultPair airportTaxiTime = Airports_HighAvgTaxiTime.pollFirst();
	               context.write(new Text(airportTaxiTime.airportCode), new DoubleWritable(airportTaxiTime.avgTaxiTime));
	             }
			 
			 context.write(new Text("Airport with Lowest Average TaxiTime"), null);
	    	  while (!Airports_LowAvgTaxiTime.isEmpty())
				  {
	    		     ResultPair airportTaxiTime = Airports_LowAvgTaxiTime.pollLast();
		             context.write(new Text(airportTaxiTime.airportCode), new DoubleWritable(airportTaxiTime.avgTaxiTime));
	               }
			 

			}

		
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(TaxiTimeInOut.class);

		job.setMapperClass(TaxiInOutMap.class);
		job.setReducerClass(TaxiInOutReduce.class);
		//job.setCombinerClass(Combiner.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.waitForCompletion(true);
		
	}


		public static class ResultPair implements Comparable<ResultPair>
			{
				
				String airportCode;
				double avgTaxiTime;
				
				ResultPair(String airportCode,double avgTaxiTime)
					{
					this.airportCode = airportCode;
					this.avgTaxiTime = avgTaxiTime;
					}

				public int compareTo(ResultPair resultPair) 
					{
					if(this.avgTaxiTime <= resultPair.avgTaxiTime)
					    return 1;
					else
						return -1;
					}
				
		}

}