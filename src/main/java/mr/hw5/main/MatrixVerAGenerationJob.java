package mr.hw5.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * This class generate the Matrix representation of a graph for row by column multiplication.
 * Output matrix can be looked as a in link graph with page rank contribution ratios.
 * Dangling nodes are handled separately.
 * @author dspatel
 *
 */
public class MatrixVerAGenerationJob 
{
	/**
	 * Mapper class
	 * @author dspatel
	 *
	 */
	public static class MatrixVerAGenerationMapper extends Mapper<Object, Text, Text, Text> 
	{
		/**
		 * This method emits the in link to a node along with the dangling node flag
		 */
		@Override
		public void map(Object key, Text line, Context context) throws IOException, InterruptedException 
		{
			String strArr[] = line.toString().split(":");
			
			if(strArr.length > 1 && null != strArr[1] && !"".equals(strArr[1]))
			{
				String adjacencyList[] = strArr[1].split(",");
				for(String adjacentNodeId : adjacencyList)
				{
					// emit each adjacent node with nodeId as key and the source node as value.
					context.write(new Text(adjacentNodeId), new Text(strArr[0] + "-" + adjacencyList.length));
				}
				
				// emit the current node as key and blank as a value to handle node with no in link case				
				context.write(new Text(strArr[0]), new Text());
			}
			else
			{
				// emit a flag to notify that the current node is a dangling node
				context.write(new Text(strArr[0]), new Text("DANGLING~NODE"));
			}
		}
	}
	
	/**
	 * Reducer class
	 * @author dspatel
	 *
	 */
	public static class MatrixVerAGenerationReducer extends Reducer<Text, Text, Text, NullWritable> 
	{
		private MultipleOutputs<Text, NullWritable> mos;
		private Double defaultPageRank = 0.0;
		
		@Override
		protected void setup(Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException 
		{
			super.setup(context);
			mos = new MultipleOutputs<Text, NullWritable>(context);
			defaultPageRank = 1.0/Integer.parseInt(context.getConfiguration().get("NUMBER_OF_PAGES"));
		}
		
		/**
		 * Reducer emits the node with its in link contribution in graph folder and node with its initial page 
		 * rank along with dangling node flag in page rank folder.
		 */
		public void reduce(Text nodeId, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			StringBuffer buf = new StringBuffer();
			boolean isDanglingNode = false;
			
			buf.append(nodeId + ":");
			
			for(Text text : values)
			{
				if(text.getLength()>0)
				{
					if(text.toString().equalsIgnoreCase("DANGLING~NODE"))
						isDanglingNode = true;
					else
						buf.append(text + ",");
				}
			}
			
			if(buf.charAt(buf.length()-1) == ',')
			{
				buf = buf.deleteCharAt(buf.length()-1);
			}

			if(isDanglingNode)
				mos.write(new Text(nodeId + ":" + defaultPageRank + ":D"), NullWritable.get(), "PageRank-I0/PageRank-");	
			else
				mos.write(new Text(nodeId + ":" + defaultPageRank), NullWritable.get(), "PageRank-I0/PageRank-");
			
			mos.write(new Text(buf.toString()), NullWritable.get(), "Matrix/Matrix");
		}
		
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException 
		{
			super.cleanup(context);
			mos.close();
		}
	}
	
	/**
	 * This method executes the Matrix Generation Job
	 * @param outputPath
	 * @param numberOfPages
	 * @return
	 * @throws Exception
	 */
	public boolean runMatrixVerAGenerationJob(String outputPath, Integer numberOfPages) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("NUMBER_OF_PAGES", numberOfPages.toString());
		
		Job job = Job.getInstance(conf, "MatrixVerAGenerationJob");
		job.setJarByClass(MatrixVerAGenerationJob.class);

		job.setMapperClass(MatrixVerAGenerationMapper.class);
		job.setReducerClass(MatrixVerAGenerationReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(outputPath + "/PreProcessing/Graph"));
		FileOutputFormat.setOutputPath(job, new Path(outputPath + "/MatrixGeneration"));
		
		MultipleOutputs.addNamedOutput(job, "Matrix", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "PageRank", TextOutputFormat.class, Text.class, NullWritable.class);
		
		return job.waitForCompletion(true);
	}

}
