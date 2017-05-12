package mr.hw5.main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

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

import mr.hw5.datatypes.MyTreeMap;

/**
 * Top-k Job: From the output of the last PageRank iteration this class finds the k(100) 
 * pages with the highest PageRank and output them, along with their ranks, from highest 
 * to lowest. It also converts the NodeId to NodeName.
 * @author dspatel
 *
 */
public class TopKJob 
{
	/**
	 * Mapper Class
	 * This class uses in-mapper combining concept to keep track of local topK pages
	 * @author dspatel
	 *
	 */
	public static class TopKMapper extends Mapper<Object, Text, NullWritable, Text> 
	{
		// MyTreeMap is used to keep track of local topK records which keeps two values with 
		// same key in list so handles duplicate page rank case.
		private MyTreeMap topRankNodes;
		private int k = 100;

		/**
		 * This setup function initializes MyTreeMap and k from context configuration
		 */
		@Override
		protected void setup(Mapper<Object, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException 
		{
			super.setup(context);
			topRankNodes = new MyTreeMap();
			k = Integer.parseInt(context.getConfiguration().get("K"));
		}
		
		/**
		 * This map function keeps track of local top k page rank nodes using MyTreeMap.
		 */
		@Override
		public void map(Object key, Text line, Context context) throws IOException, InterruptedException 
		{
			String strArr[] = line.toString().split(":");
			topRankNodes.add(Double.parseDouble(strArr[1]), Integer.parseInt(strArr[0]));
			
			if (topRankNodes.size() > k) 
			{
				// removes the lowest page rank node from the map
				topRankNodes.remove();
			}
		}

		/**
		 * cleanup function emits the local top k page rank nodes.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException 
		{
			Iterator<Entry<Double, List<Integer>>> it = topRankNodes.getMap().entrySet().iterator();
			
			while(it.hasNext())
			{
				Entry<Double, List<Integer>> entry = it.next();
				for(Integer nodeId : entry.getValue())
				{
					context.write(NullWritable.get(), new Text(nodeId + ":" + entry.getKey()));
				}
			}
		}
	}
	
	/**
	 * Reducer Class
	 * @author dspatel
	 *
	 */
	public static class TopKReducer extends Reducer<NullWritable, Text, Text, NullWritable> 
	{
		private Integer numberOfPages;
		private String[] nodeIdNameMap;
		
		/**
		 * This method generates the nodeId to nodeName map in memory from distributed cache.
		 */
		@Override
		protected void setup(Reducer<NullWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException 
		{
			super.setup(context);
			numberOfPages = Integer.parseInt(context.getConfiguration().get("NUMBER_OF_PAGES"));
			
			URI[] fileURIs = context.getCacheFiles();
			if (fileURIs != null && fileURIs.length > 0) 
			{
				nodeIdNameMap = new String[numberOfPages];
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
							String strArr[] = line.split("~");
							
							if(null != strArr && strArr.length > 1)
							{
								nodeIdNameMap[Integer.parseInt(strArr[1])] = strArr[0];
							}
						}
		            }   
					br.close(); 
				}
		    }

		}
		/**
		 * This function keeps track of top k page rank nodes using MyTreeMap and after
		 * iterating all local map topk lists (values list), it emits the top k page rank 
		 * values (from highest to lowest) with nodeId (page name).
		 */
		@Override
		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			// MyTreeMap is used to keep track of local topK records which keeps two values with 
			// same key in list so handles duplicate page rank case.
			MyTreeMap topRankNodes = new MyTreeMap();
			
			int k = Integer.parseInt(context.getConfiguration().get("K"));
			
			for (Text value : values) 
			{
				// decode the node from text
				String strArr[] = value.toString().split(":");
				topRankNodes.add(Double.parseDouble(strArr[1]), Integer.parseInt(strArr[0]));
				
				if (topRankNodes.size() > k) 
				{
					// removes the lowest page rank node from the map
					topRankNodes.remove();
				}
			}

			// emit the top k page rank values (from highest to lowest) with nodeId (page name)
			for (Double pageRank : topRankNodes.getMap().descendingMap().keySet()) 
			{
				for(Integer nodeId : topRankNodes.getMap().get(pageRank))
				{
					context.write(new Text(nodeIdNameMap[nodeId] + ":" + pageRank), NullWritable.get());
				}
			}
		}
	}

	/**
	 * This method executes the Map Only TopKJob
	 * @param inputPath
	 * @param outputPath
	 * @param k
	 * @return
	 * @throws Exception
	 */
	public boolean runTopKJob(String outputPath, Integer k, Integer numberOfPages, Integer iteration) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("K", k.toString());
		conf.set("NUMBER_OF_PAGES", numberOfPages.toString());
		
		Job job = Job.getInstance(conf, "TopKJob");
		job.setJarByClass(TopKJob.class);
		
		// Read Node Id to Node Name map from distributed cache
		Path dir = new Path(new URI(outputPath + "/PreProcessing/NodeIdMap"));
		FileSystem fs = dir.getFileSystem(conf);
		FileStatus[] fileStatus = fs.listStatus(dir);
		for (FileStatus status : fileStatus) 
		{
			job.addCacheFile(status.getPath().toUri());
	    }

		FileInputFormat.addInputPath(job, new Path(outputPath + "/PageRank/PageRank-I" + iteration));
		FileOutputFormat.setOutputPath(job, new Path(outputPath + "/TopKPageRanks"));

		job.setMapperClass(TopKMapper.class);
		job.setReducerClass(TopKReducer.class);
		
		// one reducer only
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true);
	}
}