package mr.hw5.main;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
import org.apache.log4j.BasicConfigurator;

import mr.hw5.datatypes.Node;
import mr.hw5.utility.Counters;
import mr.hw5.utility.MyParser;

/**
 * Pre-processing Job: This class parses input Wikipedia data into a graph represented as
 * adjacency lists. The page names are used as node and link IDs. Any path information from the
 * beginning, .html suffix from the end and pages and links containing a tilde (~) character 
 * are discarded.
 * @author dspatel
 *
 */
public class PreProcessingJob
{
	/**
	 * Mapper class with only map function
	 * @author dspatel
	 */
	public static class PreProcessingMapper extends Mapper<Object, Text, Text, Text> 
	{
		private Text emptyAdjacencyList = new Text();
		
		/**
		 * Map function parses each line to nodeId and adjacencyList graph representation.
		 * It emits the node with its adjacencyList as text and additionally it emits each 
		 * adjacent node from the adjacencyList because any of them can be a dangling node and
		 * we need to count them. Adjacent nodes are emitted with empty adjacencyList. 
		 */
		@Override
		public void map(Object key, Text line, Context context) throws IOException, InterruptedException 
		{
			if(null != line)
			{
				Node node = MyParser.parse(line.toString());
				
				if(null != node)
				{
					// Emit nodeId with its adjacencyList
					context.write(node.getNodeIdAsText(), node.getAdjacencyListAsText());
						
					// Emit each adjacentNode with empty adjacencyList
					for(String adjacentNodeId : node.getAdjacencyList())
					{
						context.write(new Text(adjacentNodeId), emptyAdjacencyList);
					}
				}
			}
		}
	}
	
	/**
	 * Combiner class with only reduce function
	 * @author dspatel
	 */
	public static class PreProcessingCombiner extends Reducer<Text, Text, Text, Text> 
	{
		/**
		 * This method combines the adjacencyLists of a specific node and emits the 
		 * first non empty adjacencyList if found else emits empty adjacencyList against
		 * nodeId. We can have duplicate documents(lines) with same nodeId in input so only
		 * the first document(line) is considered for adjacencyList.
		 */
		public void reduce(Text nodeId, Iterable<Text> adjacencyListValues, Context context) throws IOException, InterruptedException 
		{
			Text adjacencyList = new Text();
			
			for(Text adjacencyListVal : adjacencyListValues)
			{
				if(!"".equals(adjacencyListVal.toString()))
				{
					// break the loop after getting the first non empty adjacencyListVal
					adjacencyList.set(adjacencyListVal);
					break;
				}
			}

			// emit nodeId with its adjacencyList
			context.write(nodeId, adjacencyList);
		}
	}
	
	/**
	 * Reducer class with setup, reduce and cleanup function
	 * It uses the in-mapper combining concept (in reducer) to calculate the total no of pages
	 * @author dspatel
	 */
	public static class PreProcessingReducer extends Reducer<Text, Text, Text, NullWritable> 
	{
		private long localNumberOfPages;
		private HashMap<String, Long> nodeIdMap;
		private Long currentId;
		private MultipleOutputs<Text, NullWritable> mos;
		
		/**
		 * Initialize the localNumberOfPages for reduce task
		 * It also creates the node Name to Id map.
		 */
		@Override
		protected void setup(Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException 
		{
			super.setup(context);
			localNumberOfPages = 0;
			nodeIdMap = new HashMap<String, Long>();
			currentId = 0L;
			mos = new MultipleOutputs<Text, NullWritable>(context);
		}
		
		/**
		 * This method combines the adjacencyLists of a specific node and emits the 
		 * first non empty adjacencyList if found else emits empty adjacencyList along
		 * with nodeId. We can have duplicate documents(lines) with same nodeId in input
		 * so only the first document(line) is considered for adjacencyList.
		 */
		public void reduce(Text nodeId, Iterable<Text> adjacencyListValues, Context context) throws IOException, InterruptedException 
		{
			Text adjacencyList = new Text();
			
			for(Text adjacencyListVal : adjacencyListValues)
			{
				if(!"".equals(adjacencyListVal.toString()))
				{
					adjacencyList.set(adjacencyListVal);
					break;
				}
			}

			// increment the number of pages
			localNumberOfPages++; 
			Node node = new Node(nodeId, adjacencyList);
			
			StringBuffer buf = new StringBuffer();
			
			// check nodeId in map and if present replace it with id else assign it a new id
			if(nodeIdMap.containsKey(node.getNodeId()))
			{
				buf.append(nodeIdMap.get(node.getNodeId()));
			}
			else
			{
				buf.append(currentId);
				nodeIdMap.put(node.getNodeId(),currentId);
				currentId++;
			}
			
			buf.append(":");
			
			// check adjacentNodeId in map and if present replace it with id else assign it a new id
			for(String adjacentNodeId : node.getAdjacencyList())
			{
				if(nodeIdMap.containsKey(adjacentNodeId))
				{
					buf.append(nodeIdMap.get(adjacentNodeId));
				}
				else
				{
					buf.append(currentId);
					nodeIdMap.put(adjacentNodeId,currentId);
					currentId++;
				}
				buf.append(",");
			}
			
			if(buf.charAt(buf.length()-1) == ',')
			{
				buf = buf.deleteCharAt(buf.length()-1);
			}
			
			mos.write(new Text(buf.toString()), NullWritable.get(), "Graph/Graph");
		}
		
		/**
		 * This function increments the global counter by localNumberOfPages count.
		 * It also writes the Node Name to Node Id map to file.
		 */
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException 
		{
			super.cleanup(context);
			context.getCounter(Counters.NUMBER_OF_PAGES).increment(localNumberOfPages);
			
			Iterator<Map.Entry<String, Long>> it = nodeIdMap.entrySet().iterator();
		    while (it.hasNext()) 
		    {
		        Map.Entry<String, Long> pair = (Map.Entry<String, Long>) it.next();
		        mos.write(new Text(pair.getKey() + "~" + pair.getValue()), NullWritable.get(), "NodeIdMap/NodeIdMap");
		    }
			 mos.close();
		}
	}

	/**
	 * This method executes the PreProcessingJob and returns the job object if job
	 * is successful else null.
	 * @param inputPath
	 * @param outputPath
	 * @return
	 * @throws Exception
	 */
	public Job runPreProcessingJob(String inputPath, String outputPath) throws Exception
	{
		Configuration conf = new Configuration();
		BasicConfigurator.configure();
		
		Job job = Job.getInstance(conf, "PreProcessingJob");
		job.setJarByClass(PreProcessingJob.class);

		job.setMapperClass(PreProcessingMapper.class);
		job.setCombinerClass(PreProcessingCombiner.class);
		job.setReducerClass(PreProcessingReducer.class);
		job.setNumReduceTasks(1);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath + "/PreProcessing"));
		
		// Two separate outputs, one for writing graph and one for node Id to node Name Map
		MultipleOutputs.addNamedOutput(job, "Graph", TextOutputFormat.class, Text.class, NullWritable.class);
		MultipleOutputs.addNamedOutput(job, "NodeIdMap", TextOutputFormat.class, Text.class, NullWritable.class);

		if(job.waitForCompletion(true))
		{
			return job;
		}
		
		return null;
	}
}