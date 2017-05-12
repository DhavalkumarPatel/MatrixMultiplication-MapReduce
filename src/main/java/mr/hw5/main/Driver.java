package mr.hw5.main;

import java.util.Date;

import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import mr.hw5.utility.Counters;

/**
 * This class runs PreProcessingJob, MatrixGenerationJob, PageRankJob and TopKJob
 * in sequence and logs the running time of each job.
 * @author dspatel
 *
 */
public class Driver 
{
	public static void main(String[] args) throws Exception
	{
		Logger logger = Logger.getLogger(Driver.class);
		try
		{
			//START - Initialize the local variables from args argument
			if (args.length != 6) 
			{
				logger.error("Usage: Driver <input path> <output path> <alpha> <No of Iteration> <K for TopK> <Version>");
				System.exit(-1);
			}
			String inputPath = args[0];
			String outputPath = args[1];
			Double alpha = Double.parseDouble(args[2]);
			int noOfIteration = Integer.parseInt(args[3]);
			int kForTopK = Integer.parseInt(args[4]);
			String version = args[5];
			
			Date t0 = new Date();
			
			
			//START - PreProcessingJob
			logger.info("PreProcessingJob Started.");
			
			// run the preProcessingJob and get the total number of pages from global counters
			Job preProcessingJob = new PreProcessingJob().runPreProcessingJob(inputPath, outputPath);
			Long numberOfPages = preProcessingJob.getCounters().findCounter(Counters.NUMBER_OF_PAGES).getValue();
			
			logger.info("PreProcessingJob completed successfully.");
			//END - PreProcessingJob
			
			
			Date t1 = new Date();
			
			
			//START - MatrixGenerationJob
			logger.info("MatrixGenerationJob Started.");
			
			// run the MatrixGeneration for specific version and generate the Node Matrix and/or Initial Page Rank Matrix
			if("A".equalsIgnoreCase(version))
				new MatrixVerAGenerationJob().runMatrixVerAGenerationJob(outputPath, numberOfPages.intValue());
			else
				new MatrixVerBGenerationJob().runMatrixVerBGenerationJob(outputPath, numberOfPages.intValue());
			
			logger.info("MatrixGenerationJob completed successfully.");
			//END - MatrixGenerationJob
			
			
			Date t2 = new Date();
			

			//START - PageRankJob
			for(int i = 1; i <= noOfIteration; i++)
			{
				logger.info("PageRankJob Iteration-" + i + " Started.");
				
				// run the page rank job
				if("A".equalsIgnoreCase(version))
					new PageRankVerAJob().runPageRankVerAJob(outputPath, i, numberOfPages.intValue(), alpha);
				else
					new PageRankVerBJob().runPageRankVerBJob(outputPath, i, numberOfPages.intValue(), alpha);

				logger.info("PageRankJob Iteration-" + i + " completed successfully.");
			}
			//END - PageRankJob
			
			
			Date t3 = new Date();
			
			
			//START - TopKJob
			logger.info("TopKJob Started.");
			
			// run the TopKJob
			new TopKJob().runTopKJob(outputPath, kForTopK, numberOfPages.intValue(), noOfIteration);
			
			logger.info("TopKJob completed successfully.");
			//END - TopKJob
			
			
			Date t4 = new Date();
			
			logger.info("Total number of pages = " + numberOfPages);
			logger.info("PreProcssing Running Time = " + ((t1.getTime() - t0.getTime()) /1000) + " seconds.");
			logger.info("MatrixGenerationJob Running Time = " + ((t2.getTime() - t1.getTime()) /1000) + " seconds.");
			logger.info("PageRankJob Running Time = " + ((t3.getTime() - t2.getTime()) /1000) + " seconds.");
			logger.info("TopKJob Running Time = " + ((t4.getTime() - t3.getTime()) /1000) + " seconds.");
			logger.info("Total Running Time = " + ((t4.getTime() - t0.getTime()) /1000) + " seconds.");
		}
		catch(Exception e)
		{
			logger.error(e.getMessage());
			logger.error("PageRank job sequence failed.");
			throw e;
		}
	}
}
