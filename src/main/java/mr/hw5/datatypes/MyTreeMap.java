package mr.hw5.datatypes;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * This class is used to track Top K records for specific map/reduce task.
 * There can be multiple nodes with the same page rank and if we use TreeMap
 * directly then it will not keep the duplicate records. It uses TreeMap only
 * but keeps value as a list of pages with same page rank. Add and Remove methods
 * are modifies as per our needs. 
 * @author dspatel
 *
 */
public class MyTreeMap 
{
	private TreeMap<Double, List<Integer>> map;
	private int size;
	
	public MyTreeMap() 
	{
		 map = new TreeMap<Double, List<Integer>>();
		 size = 0;
	}
	
	/**
	 * Adds the node against pageRank in TreeMap.
	 * If pageRank is already present in the map then it adds the node in
	 * current node list of that pageRank from the map else it generates
	 * the new list with this node and adds a new entry in map.
	 * @param pageRank
	 * @param node
	 */
	public void add(Double pageRank, Integer nodeId)
	{
		List<Integer> nodeIds = map.get(pageRank);
		
		if(nodeIds == null)
		{
			nodeIds = new ArrayList<Integer>();
		}
		
		nodeIds.add(nodeId);
		map.put(pageRank, nodeIds);
		size++;
	}
	
	/**
	 * Removes the node with the lowest pageRank. If we have more than one
	 * nodes with lowest pageRank then it removes the last node from the list. 
	 */
	public void remove()
	{
		Double pageRank = map.firstKey();
		List<Integer> nodeIds = map.get(pageRank);
		
		if(nodeIds.size() > 1)
		{
			nodeIds.remove(nodeIds.size() - 1);
			map.put(pageRank, nodeIds);
		}
		else
		{
			map.remove(pageRank);
		}
		size--;
	}
	
	public TreeMap<Double, List<Integer>> getMap() 
	{
		return map;
	}
	
	public int size()
	{
		return size;
	}
}