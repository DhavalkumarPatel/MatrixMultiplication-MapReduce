package mr.hw5.datatypes;

import org.apache.hadoop.io.Text;

import mr.hw5.utility.Utility;

/**
 * This class is used to store Node's attributes provides operations to be
 * performed on Node. 
 * @author dspatel
 */
public class Node
{	
	private String nodeId;
	private String adjacencyList;
	
    public Node(String nodeId, String adjacencyList)
    {
    	this.nodeId = nodeId;
    	this.adjacencyList = adjacencyList;
    }
    
    public Node(Text nodeId, Text adjacencyList)
    {
    	this(nodeId.toString(), adjacencyList.toString());
    }
    
    public String getNodeId() 
    {
		return nodeId;
	}
    
    public Text getNodeIdAsText() 
    {
		return new Text(nodeId);
	}
    
    public String getAdjacencyListAsString() 
    {
		return adjacencyList;
	}
    
    public Text getAdjacencyListAsText() 
    {
		return new Text(adjacencyList);
	}
    
    public String[] getAdjacencyList() 
    {
		return Utility.adjacencyListStringToArray(this.adjacencyList);
	}
}