import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AvgGrowthSale {

	// Mapper Class	
	
	   public static class AvgGrowthSortMapperClass extends Mapper<LongWritable,Text,DoubleWritable,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {
	         try{
	            String[] str = value.toString().split(",");
	            double avg_growth = Double.parseDouble(str[4]);
	            context.write(new DoubleWritable(avg_growth), value);
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }

	   //Reducer class
		
	   public static class AvgGrowthSortReducerClass extends Reducer<DoubleWritable,Text,NullWritable,Text>
	   {
	      public void reduce(DoubleWritable key, Iterable <Text> values, Context context) throws IOException, InterruptedException
	      {
	         for (Text val : values)
	         {
				context.write(NullWritable.get(), val);
	         }
	      }
	   }

//Main class
	   
	   public static void main(String[] args) throws Exception {
			
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Avg Growth Sorting");
		    job.setJarByClass(AvgGrowthSale.class);
		    job.setMapperClass(AvgGrowthSortMapperClass.class);
		    job.setReducerClass(AvgGrowthSortReducerClass.class);
		    job.setMapOutputKeyClass(DoubleWritable.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}