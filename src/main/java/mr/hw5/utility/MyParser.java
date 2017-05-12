package mr.hw5.utility;

import java.net.URLDecoder;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import mr.hw5.datatypes.Node;

/**
 * This class parses the input html document to nodeId and its adjacency List
 */
public class MyParser 
{
	private static Pattern namePattern;
	private static Pattern linkPattern;
	
	static 
	{
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
	}
	
	public static Node parse(String line)
	{
		try
		{
			int delimLoc = line.indexOf(':');
			
			// Parse nodeId
			String nodeId = line.substring(0, delimLoc);
			Matcher matcher = namePattern.matcher(nodeId);
			if (!matcher.find()) 
			{
				// Skip this html file, nodeId contains (~).
				return null;
			}
			
			// Parse adjacencyList
			String html = line.substring(delimLoc + 1);
			Set<String> adjacencyList = new HashSet<String>();
			
			Document doc = Jsoup.parse(html);
			
			for(Element element : doc.getElementsByAttributeValue("id", "bodyContent"))
			{
				if(null != element && null != element.tagName() && element.tagName().equalsIgnoreCase("div"))
				{
					for(Element linkElem : element.select("a[href]"))
					{
						String link = linkElem.attr("href");
						if (null != link) 
						{
							link = URLDecoder.decode(link, "UTF-8");
							Matcher linkMatcher = linkPattern.matcher(link);
							if (linkMatcher.find()) 
							{
								adjacencyList.add(linkMatcher.group(1));
							}
						}
					}
				}
			}
			
			// remove self links
			adjacencyList.remove(nodeId);
			
			return new Node(nodeId, Utility.adjacencyListSetToString(adjacencyList));
		}
		catch(Exception e)
		{
			System.err.println(e.getMessage());
		}
		return null;
	}
}
