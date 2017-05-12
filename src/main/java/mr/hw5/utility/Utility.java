package mr.hw5.utility;

import java.util.Set;

import org.apache.commons.lang.StringUtils;

/**
 * This class provides different utility methods to all other programs.
 * @author dspatel
 *
 */
public class Utility 
{
	private static final String TILD = "~";
	
	/**
	 * This method converts the given adjacencyListString to array
	 * @param adjacencyListString
	 * @return
	 */
	public static String[] adjacencyListStringToArray(String adjacencyListString)
	{
		String[] adjacencyListArray = null;
		if(null != adjacencyListString)
		{
			adjacencyListArray = StringUtils.split(adjacencyListString, TILD);
		}
		return adjacencyListArray;
	}
	
	/**
	 * This method converts the given adjacencyListSet to String
	 * @param adjacencyListSet
	 * @return
	 */
	public static String adjacencyListSetToString(Set<String> adjacencyListSet)
	{
		String adjacencyListString = null;
		if(null != adjacencyListSet)
		{
			adjacencyListString = StringUtils.join(adjacencyListSet, TILD);
		}
		return adjacencyListString;
	}
}
