package edu.ucla.cs.cs144;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;
import java.util.Date;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.text.SimpleDateFormat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.lucene.document.Document;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import edu.ucla.cs.cs144.DbManager;
import edu.ucla.cs.cs144.SearchRegion;
import edu.ucla.cs.cs144.SearchResult;

import java.io.StringWriter;

import org.w3c.dom.Element;
import org.w3c.dom.DOMException;

import javax.xml.parsers.*;
import javax.xml.transform.*;

import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.dom.DOMSource;

public class AuctionSearch implements IAuctionSearch {

	/* 
         * You will probably have to use JDBC to access MySQL data
         * Lucene IndexSearcher class to lookup Lucene index.
         * Read the corresponding tutorial to learn about how to use these.
         *
	 * You may create helper functions or classes to simplify writing these
	 * methods. Make sure that your helper functions are not public,
         * so that they are not exposed to outside of this class.
         *
         * Any new classes that you create should be part of
         * edu.ucla.cs.cs144 package and their source files should be
         * placed at src/edu/ucla/cs/cs144.
         *
         */
	
	public SearchResult[] basicSearch(String query, int numResultsToSkip, 
			int numResultsToReturn) {
		// TODO: Your code here!
		SearchResult[] resultList = new SearchResult[0];
		try
		{
			IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(new File("/var/lib/lucene/"))));
			TopDocs results = searcher.search(new QueryParser("Content", new StandardAnalyzer()).parse(query), (numResultsToReturn + numResultsToSkip));
			if ((results.totalHits < numResultsToSkip) || (numResultsToReturn <= 0))
			{
				
				return resultList;
			}
			ArrayList<SearchResult> temp = new ArrayList<SearchResult>();
			int counter = 0;
			//ScoreDoc[] scores = results.scoreDocs;
			Document doc = null;
			for (int i = numResultsToSkip; i < results.totalHits; i++)
			{
				if (counter >= numResultsToReturn)
				{
					break;
				}
				doc = searcher.doc(results.scoreDocs[i].doc);
				temp.add(new SearchResult(doc.get("ItemID"), doc.get("ItemName")));
				counter++;
			}
			resultList = new SearchResult[temp.size()];
			resultList = temp.toArray(resultList);
		}
        catch (Exception e)
        {
            System.err.println(e.getMessage());
        }
		return resultList;
	}

	public SearchResult[] spatialSearch(String query, SearchRegion region,
			int numResultsToSkip, int numResultsToReturn) {
		// TODO: Your code here!
		SearchResult[] resultList = new SearchResult[0];
		try
		{
			Connection connection = DbManager.getConnection(true);
			SearchResult[] results = basicSearch(query, numResultsToSkip, numResultsToReturn);
			System.out.println(results.length);
			ArrayList<SearchResult> temp = new ArrayList<SearchResult>();
			ResultSet locations = null;
			String location = "";
			String[] locationList = null;
			int counter = 0;
			for (SearchResult currResult : results)
			{
				if (counter > numResultsToReturn)
				{
					break;
				}
				locations = connection.createStatement().executeQuery("SELECT ItemID, AsText(Location) FROM SpatialTable WHERE ItemID =" + currResult.getItemId()); 
				if (locations.next())
				{
					location = locations.getString("AsText(Location)");
					System.out.println(location);
					locationList = location.substring(6, location.length() - 1).split(" ");
					if (Double.parseDouble(locationList[0]) <= region.getRx() && Double.parseDouble(locationList[0]) >= region.getLx())
					{
						if (Double.parseDouble(locationList[1]) <= region.getRy() && Double.parseDouble(locationList[1]) >= region.getLy())
						{
							if (counter < numResultsToSkip)
								continue;
							temp.add(currResult);
							counter++;
						}
					}
				}
			}
			resultList = new SearchResult[temp.size()];
			resultList = temp.toArray(resultList);
		}
        catch (Exception e)
        {
            System.err.println(e.getMessage());
        }
		return resultList;
	}

	public String getXMLDataForItemId(String itemId) {
		// TODO: Your code here
		String result = "";
		try
		{
			Connection conn = DbManager.getConnection(true);
			Statement stmt = conn.createStatement();
			ResultSet item = stmt.executeQuery("SELECT * FROM Items WHERE ItemID =" + itemId);
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			org.w3c.dom.Document document = builder.newDocument();
			Element root = document.createElement("Item");
			root.setAttribute("ItemID", itemId);
			document.appendChild(root);
			if (item.next())
			{
				Element name = document.createElement("Name");
				name.appendChild(document.createTextNode(item.getString("ItemName")));
				root.appendChild(name);
				Statement stmt2 = conn.createStatement();
				ResultSet cats = stmt2.executeQuery("SELECT Category FROM ItemCategory WHERE ItemID =" + itemId);
				Element category;
				while (cats.next())
				{
					category = document.createElement("Category");
					category.appendChild(document.createTextNode(cats.getString("Category")));
					root.appendChild(category);
				}
				Element currently = document.createElement("Currently");
				currently.appendChild(document.createTextNode("$" + item.getString("currentHighestBid")));
				root.appendChild(currently);
				Element buyprice = document.createElement("BuyPrice");
				String buypriceTemp = item.getString("BuyPrice");
				if (item.wasNull())
				{
					buypriceTemp = "";
				}
				else
				{
					buyprice.appendChild(document.createTextNode("$" + buypriceTemp));
				}
				root.appendChild(buyprice);
				Element firstbid = document.createElement("FirstBid");
				firstbid.appendChild(document.createTextNode("$" + item.getString("FirstBid")));
				root.appendChild(firstbid);
				Element numberofbids = document.createElement("NumberOfBids");
				numberofbids.appendChild(document.createTextNode(item.getString("NumberOfBids")));
				root.appendChild(numberofbids);
				Statement stmt3 = conn.createStatement();
				ResultSet bidset = stmt3.executeQuery("SELECT * FROM Bids, Users WHERE Bids.UserID = Users.UserID AND Bids.ItemID = " + itemId );
				Element bids = document.createElement("Bids");
				while (bidset.next())
				{
					Element bid = document.createElement("Bid");
					Element bidder = document.createElement("Bidder");
					bidder.setAttribute("Rating", bidset.getString("BidderRating"));
					bidder.setAttribute("UserID", bidset.getString("BidderID"));
					Element location = document.createElement("Location");
					location.appendChild(document.createTextNode(bidset.getString("Address")));
					bidder.appendChild(location);
					Element country = document.createElement("Country");
					country.appendChild(document.createTextNode(bidset.getString("Country")));
					bidder.appendChild(country);
					bid.appendChild(bidder);
					SimpleDateFormat currentForm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					SimpleDateFormat desiredForm = new SimpleDateFormat("MMM-dd-yy HH:mm:ss");
					Date tempD = currentForm.parse(bidset.getString("Time"));
					Element time = document.createElement("Time");
					time.appendChild(document.createTextNode(desiredForm.format(tempD)));
					bid.appendChild(time);
					Element amount = document.createElement("Amount");
					amount.appendChild(document.createTextNode(bidset.getString("Amount")));
					bid.appendChild(amount);
					bids.appendChild(bid);
				}
				root.appendChild(bids);
				Element location = document.createElement("Location");
				String temp = item.getString("Latitude");
				if (item.wasNull() == false)
				{
					location.setAttribute("Latitude", item.getString("Latitude"));
					location.setAttribute("Longitude", item.getString("Longitude"));
				}
				location.appendChild(document.createTextNode(item.getString("Address")));
				root.appendChild(location);
				Element country = document.createElement("Country");
				country.appendChild(document.createTextNode(item.getString("Country")));
				root.appendChild(country);
				SimpleDateFormat currentForm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				SimpleDateFormat desiredForm = new SimpleDateFormat("MMM-dd-yy HH:mm:ss");
				Date tempDate = currentForm.parse(item.getString("StartTime"));;
				Element started = document.createElement("Started");
				started.appendChild(document.createTextNode(desiredForm.format(tempDate)));
				root.appendChild(started);
				tempDate = currentForm.parse(item.getString("EndTime"));
				Element ends = document.createElement("Ends");
				ends.appendChild(document.createTextNode(desiredForm.format(tempDate)));
				root.appendChild(ends);
				Statement stmt4 = conn.createStatement();
				ResultSet sellerset = stmt4.executeQuery("SELECT * FROM Items, Users WHERE Items.UserID = Users.UserID AND Items.ItemID = " + itemId);
				if (sellerset.next())
				{
					Element seller = document.createElement("Seller");
					seller.setAttribute("Rating", sellerset.getString("SellerRating"));
					seller.setAttribute("UserID", sellerset.getString("UserID"));
					root.appendChild(seller);
				}
				Element description = document.createElement("Description");
				description.appendChild(document.createTextNode(item.getString("Description")));
				root.appendChild(description);
				TransformerFactory tfactory = TransformerFactory.newInstance();
				Transformer transformer = tfactory.newTransformer();
				transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
				transformer.setOutputProperty(OutputKeys.INDENT, "yes");
				StringWriter swriter = new StringWriter();
				DOMSource dsource = new DOMSource(document);
				StreamResult sresult = new StreamResult(swriter);
				transformer.transform(dsource, sresult);
				result = swriter.toString();
			}
			else
			{
				return result;
			}
		}
        catch (Exception e)
        {
            System.err.println(e.getMessage());
        }
		return result;
	}
	
	public String echo(String message) {
		return message;
	}

}
