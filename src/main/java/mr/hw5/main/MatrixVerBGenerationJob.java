package mr.hw5.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This class generates initial page rank for each node with dangling flag.
 * There is no need to generate the M' here as the graph is already in that format.
 * @author dspatel
 *
 */
public class MatrixVerBGenerationJob 
{
	/**
	 * Mapper class
	 * @author dspatel
	 *
	 */
	public static class MatrixVerBGenerationMapper extends Mapper<Object, Text, Text, NullWritable> 
	{
		private Double defaultPageRank = 0.0;
		
		@Override
		protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException 
		{
			super.setup(context);
			defaultPageRank = 1.0/Integer.parseInt(context.getConfiguration().get("NUMBER_OF_PAGES"));
		}
		/**
		 * This method reads each node with its adjacency node and emits its initial page rank with dangling flag
		 */
		@Override
		public void map(Object key, Text line, Context context) throws IOException, InterruptedException 
		{
			String strArr[] = line.toString().split(":");
			
			StringBuffer buf = new StringBuffer();
			buf.append(strArr[0] + ":" + defaultPageRank);
			
			if(!(strArr.length > 1 && null != strArr[1] && !"".equals(strArr[1])))
			{
				buf.append(":D");
			}
			
			context.write(new Text(buf.toString()), NullWritable.get());
		}
	}
	
	/**
	 * This method executes the Matrix Generation Job
	 * @param outputPath
	 * @param numberOfPages
	 * @return
	 * @throws Exception
	 */
	public boolean runMatrixVerBGenerationJob(String outputPath, Integer numberOfPages) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("NUMBER_OF_PAGES", numberOfPages.toString());
		
		Job job = Job.getInstance(conf, "MatrixVerBGenerationJob");
		job.setJarByClass(MatrixVerBGenerationJob.class);

		job.setMapperClass(MatrixVerBGenerationMapper.class);
		job.setNumReduceTasks(0); // Map Only Jon
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(outputPath + "/PreProcessing/Graph"));
		FileOutputFormat.setOutputPath(job, new Path(outputPath + "/MatrixGeneration/PageRank-I0"));
		
		return job.waitForCompletion(true);
	}

}
