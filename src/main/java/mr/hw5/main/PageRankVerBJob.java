package mr.hw5.main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This class calculates the Page Rank of each node by doing column by row matrix
 * multiplication. It does not uses the column by row algorithm instead it uses 
 * approach like replicated join to perform matrix multiplication.
 * @author dspatel
 *
 */
public class PageRankVerBJob
{
	/**
	 * Mapper Class
	 * @author dspatel
	 *
	 */
	public static class PageRankVerBMapper extends Mapper<Object, Text, Text, Text> 
	{
		private Integer numberOfPages;
		private Double[] pageRanks = null;
		private Double delta = 0.0;
		
		/**
		 * This method reads the  entire page rank one column matrix from distributed cache and
		 * also calculate the delta during traversal
		 */
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException 
		{
			super.setup(context);
			numberOfPages = Integer.parseInt(context.getConfiguration().get("NUMBER_OF_PAGES"));
			pageRanks = new Double[numberOfPages];
			Double danglingRatio = (double) 1.0/ numberOfPages;
			
			
			// read each file from cache
			URI[] fileURIs = context.getCacheFiles();
			if (fileURIs != null && fileURIs.length > 0) 
			{
				for (URI fileURI : fileURIs) 
				{
					Path fp = new Path(fileURI);
					FileSystem fs = FileSystem.get(fp.toUri(), context.getConfiguration());
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(fp)));
					
					String line;
					while((line = br.readLine()) != null) 
					{
						if(null != line && !"".equals(line))
						{
							String strArr[] = line.split(":");
							
							if(null != strArr && strArr.length > 1)
							{
								Double pageRank = Double.parseDouble(strArr[1]);
								pageRanks[Integer.parseInt(strArr[0])] = pageRank;
								
								if(strArr.length > 2 && strArr[2].equalsIgnoreCase("D"))
								{
									delta += pageRank * danglingRatio;
								}
							}
						}
		            }   
					br.close(); 
				}
		    }
		}
		
		
		/**
		 * This method multiplies each column with row and emit the page rank contribution 
		 * to a node along with a dangling node flag.
		 */
		@Override
		public void map(Object key, Text line, Context context) throws IOException, InterruptedException 
		{
			String strArr[] = line.toString().split(":");
			
			Double pageRank = pageRanks[Integer.parseInt(strArr[0])];
			
			if(strArr.length > 1 && null != strArr[1] && !"".equals(strArr[1]))
			{
				String adjacentNodeIds[] = strArr[1].split(",");
				Double contributionRatio = (double)1.0/adjacentNodeIds.length;
				for(String adjacentNodeId : adjacentNodeIds)
				{
					context.write(new Text(adjacentNodeId), new Text(pageRank * contributionRatio + ""));
				}
				
				// emit delta contribution to a node
				context.write(new Text(strArr[0]), new Text(delta + ""));	
			}
			else
			{
				// emit delta contribution to a node with dangling node flag
				context.write(new Text(strArr[0]), new Text(delta + ":D"));
			}
			
		}
	}
	
	/**
	 * Reducer Class
	 * @author dspatel
	 *
	 */
	public static class PageRankVerBReducer extends Reducer<Text, Text, Text, NullWritable> 
	{
		private Integer numberOfPages = 0;
		private Double alpha = 0.0;
		
		/**
		 * This method initialize the variables from context
		 */
		@Override
		protected void setup(Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException 
		{
			super.setup(context);
			numberOfPages = Integer.parseInt(context.getConfiguration().get("NUMBER_OF_PAGES"));
			alpha = Double.parseDouble(context.getConfiguration().get("ALPHA"));
		}
		
		/**
		 * This method combines the page rank contributions to each node and emit the new page rank
		 * along with dangling node flag
		 */
		public void reduce(Text nodeId, Iterable<Text> rankContris, Context context) throws IOException, InterruptedException 
		{
			Double pageRankSum = 0.0;
			boolean isDanglingNode = false;
			
			for(Text rankContri: rankContris)
			{
				if(rankContri.toString().contains(":D"))
				{
					isDanglingNode = true;
					pageRankSum += Double.parseDouble(rankContri.toString().split(":")[0]);
				}
				else
				{
					pageRankSum += Double.parseDouble(rankContri.toString());					
				}
			}
			
			Double pageRank = (alpha/numberOfPages) + (1-alpha) * pageRankSum;
			
			if(isDanglingNode)
				context.write(new Text(nodeId + ":" + pageRank + ":D"), NullWritable.get());	
			else
				context.write(new Text(nodeId + ":" + pageRank), NullWritable.get());
		}
	}
	
	/**
	 * This method executes the Page Rank job
	 * @param outputPath
	 * @param iteration
	 * @param numberOfPages
	 * @param alpha
	 * @return
	 * @throws Exception
	 */
	public boolean runPageRankVerBJob(String outputPath, int iteration, Integer numberOfPages, Double alpha) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("NUMBER_OF_PAGES", numberOfPages.toString());
		conf.set("ALPHA", alpha.toString());

		Job job = Job.getInstance(conf, "PageRankVerBJob");
		job.setJarByClass(PageRankVerBJob.class);
		
		// Share the page rank of the previous iteration to all machine using distributed cache
		String prevPageRankPath = outputPath + "/PageRank/PageRank-I" + (iteration - 1);
		if(iteration == 1)
		{
			prevPageRankPath = outputPath + "/MatrixGeneration/PageRank-I0";
		}
		Path dir = new Path(new URI(prevPageRankPath));
		FileSystem fs = dir.getFileSystem(conf);
		FileStatus[] fileStatus = fs.listStatus(dir);
		for (FileStatus status : fileStatus) 
		{
			job.addCacheFile(status.getPath().toUri());
	    }

		FileInputFormat.addInputPath(job, new Path(outputPath + "/PreProcessing/Graph"));
		FileOutputFormat.setOutputPath(job, new Path(outputPath + "/PageRank/PageRank-I" + iteration));

		job.setMapperClass(PageRankVerBMapper.class);
		job.setReducerClass(PageRankVerBReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true);
	}
}