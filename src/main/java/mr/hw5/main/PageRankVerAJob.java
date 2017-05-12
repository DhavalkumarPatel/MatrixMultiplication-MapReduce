package mr.hw5.main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This class calculates the Page Rank of each node by doing row by column matrix
 * multiplication. It does not uses the row by column algorithm instead it uses 
 * approach like replicated join to perform matrix multiplication.
 * @author dspatel
 *
 */
public class PageRankVerAJob
{
	/**
	 * Mapper class
	 * @author dspatel
	 *
	 */
	public static class PageRankVerAMapper extends Mapper<Object, Text, Text, NullWritable> 
	{
		private Integer numberOfPages = 0;
		private Double[] pageRanks = null;
		private Double delta = 0.0;
		private Double alpha = 0.0;
		private Set<Integer> danglingNodeSet = null; 
		
		/**
		 * This method reads the  entire page rank one column matrix from distributed cache and
		 * also calculate the delta during traversal
		 */
		@Override
		protected void setup(Mapper<Object, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException 
		{
			super.setup(context);
			numberOfPages = Integer.parseInt(context.getConfiguration().get("NUMBER_OF_PAGES"));
			pageRanks = new Double[numberOfPages];
			alpha = Double.parseDouble(context.getConfiguration().get("ALPHA"));
			danglingNodeSet = new HashSet<Integer>();
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
								Integer nodeId = Integer.parseInt(strArr[0]);
								
								Double pageRank = Double.parseDouble(strArr[1]);
								pageRanks[nodeId] = pageRank;
								
								if(strArr.length > 2 && strArr[2].equalsIgnoreCase("D"))
								{
									danglingNodeSet.add(nodeId);
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
		 * This method multiplies each row with the column and emit the page rank of a node along
		 * with dangling node flag.
		 */
		@Override
		public void map(Object key, Text line, Context context) throws IOException, InterruptedException 
		{
			String strArr[] = line.toString().split(":");
			
			Double pageRankSum = delta;
			if(strArr.length > 1 && null != strArr[1] && !"".equals(strArr[1]))
			{
				String contributions[] = strArr[1].split(",");
				for(String contribution : contributions)
				{
					String inLinkNode[] = contribution.split("-");
					pageRankSum += pageRanks[Integer.parseInt(inLinkNode[0])] * ((double)1.0 / Integer.parseInt(inLinkNode[1]));
				}
			}
			Double pageRank = (alpha/numberOfPages) + (1-alpha) * pageRankSum;
				
			if(danglingNodeSet.contains(Integer.parseInt(strArr[0])))
				context.write(new Text(strArr[0] + ":" + pageRank + ":D"), NullWritable.get());				
			else
				context.write(new Text(strArr[0] + ":" + pageRank), NullWritable.get());
		}
	}
	
	/**
	 * This method executes the Page Rank Job
	 * @param outputPath
	 * @param iteration
	 * @param numberOfPages
	 * @param alpha
	 * @return
	 * @throws Exception
	 */
	public boolean runPageRankVerAJob(String outputPath, int iteration, Integer numberOfPages, Double alpha) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("NUMBER_OF_PAGES", numberOfPages.toString());
		conf.set("ALPHA", alpha.toString());

		Job job = Job.getInstance(conf, "PageRankVerAJob");
		job.setJarByClass(PageRankVerAJob.class);
		
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
		
		FileInputFormat.addInputPath(job, new Path(outputPath + "/MatrixGeneration/Matrix"));
		FileOutputFormat.setOutputPath(job, new Path(outputPath + "/PageRank/PageRank-I" + iteration));

		job.setMapperClass(PageRankVerAMapper.class);
		job.setNumReduceTasks(0); // Map Only Job

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		return job.waitForCompletion(true);
	}
}