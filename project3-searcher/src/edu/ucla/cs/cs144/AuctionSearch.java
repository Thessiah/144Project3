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
			ArrayList<SearchResult> temp = new ArrayList<SearchResult>();
			int counter = 0;
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
			SearchResult[] results = basicSearch(query, 0, Integer.MAX_VALUE);
			ArrayList<SearchResult> tempResults = new ArrayList<SearchResult>();
			ArrayList<SearchResult> returnResults = new ArrayList<SearchResult>();
			ResultSet locations = null;
			String temp = "";
			String[] location = null;
			Double latitude;
			Double longitude;
			for (int i = 0; i < results.length; i++)
			{
				locations = connection.createStatement().executeQuery("SELECT ItemID, AsText(Location) FROM SpatialTable WHERE ItemID =" + results[i].getItemId()); 
				if (locations.next())
				{
					temp = locations.getString("AsText(Location)");
					location = temp.substring(6, temp.length() - 1).split(" ");
					latitude = Double.parseDouble(location[0]);
					longitude = Double.parseDouble(location[1]);
					if (latitude <= region.getRx() && latitude >= region.getLx())
					{
						if (longitude <= region.getRy() && longitude >= region.getLy())
						{
							tempResults.add(results[i]);
						}
					}
				}
			}
			int counter = 0;
			for(int i = numResultsToSkip; i < tempResults.size(); i++)
			{
				if (counter >= numResultsToReturn)
				{
					break;
				}
				returnResults.add(tempResults.get(i));
				counter++;
			}
			resultList = new SearchResult[returnResults.size()];
			resultList = returnResults.toArray(resultList);
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
			Statement itemStatement = conn.createStatement();
			ResultSet item = itemStatement.executeQuery("SELECT * FROM Items WHERE ItemID =" + itemId);
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			org.w3c.dom.Document document = builder.newDocument();
			Element root = document.createElement("Item");
			root.setAttribute("ItemID", itemId);
			document.appendChild(root);
			SimpleDateFormat startingFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			SimpleDateFormat endFormat = new SimpleDateFormat("MMM-dd-yy HH:mm:ss");
			if (item.next())
			{
				Element name = document.createElement("Name");
				name.appendChild(document.createTextNode(item.getString("ItemName")));
				root.appendChild(name);
				Statement categoriesStatement = conn.createStatement();
				ResultSet categories = categoriesStatement.executeQuery("SELECT Category FROM ItemCategory WHERE ItemID =" + itemId);
				Element category;
				while (categories.next())
				{
					category = document.createElement("Category");
					category.appendChild(document.createTextNode(categories.getString("Category")));
					root.appendChild(category);
				}
				Element currently = document.createElement("Currently");
				currently.appendChild(document.createTextNode("$" + item.getString("CurrentHighestBid")));
				root.appendChild(currently);
				String buyPriceString = item.getString("BuyPrice");
				if (!item.wasNull())
				{
					Element buyPrice = document.createElement("Buy_Price");
					buyPrice.appendChild(document.createTextNode("$" + buyPriceString));
					root.appendChild(buyPrice);
				}
				Element firstBid = document.createElement("First_Bid");
				firstBid.appendChild(document.createTextNode("$" + item.getString("FirstBid")));
				root.appendChild(firstBid);
				Element numberOfBids = document.createElement("Number_of_Bids");
				numberOfBids.appendChild(document.createTextNode(item.getString("NumberOfBids")));
				root.appendChild(numberOfBids);
				Statement bidsStatement = conn.createStatement();
				ResultSet bidSet = bidsStatement.executeQuery("SELECT * FROM Bids, Users WHERE Bids.UserID = Users.UserID AND Bids.ItemID = " + itemId );
				Element bids = document.createElement("Bids");
				while (bidSet.next())
				{
					Element bid = document.createElement("Bid");
					Element bidder = document.createElement("Bidder");
					bidder.setAttribute("Rating", bidSet.getString("BuyerRating"));
					bidder.setAttribute("UserID", bidSet.getString("UserID"));
					
					String addressText = bidSet.getString("Address");
					if(!bidSet.wasNull())
					{
						Element location = document.createElement("Location");
						location.appendChild(document.createTextNode(addressText));
						bidder.appendChild(location);
					}
					String countryText = bidSet.getString("Country");
					if(!bidSet.wasNull())
					{
						Element country = document.createElement("Country");
						country.appendChild(document.createTextNode(countryText));
						bidder.appendChild(country);
					}
					bid.appendChild(bidder);
					Date date = startingFormat.parse(bidSet.getString("BidTime"));
					Element time = document.createElement("Time");
					time.appendChild(document.createTextNode(endFormat.format(date)));
					bid.appendChild(time);
					Element amount = document.createElement("Amount");
					amount.appendChild(document.createTextNode(bidSet.getString("Amount")));
					bid.appendChild(amount);
					bids.appendChild(bid);
				}
				root.appendChild(bids);
				Element location = document.createElement("Location");
				String locationString = item.getString("Latitude");
				if (!item.wasNull())
				{
					location.setAttribute("Latitude", item.getString("Latitude"));
					location.setAttribute("Longitude", item.getString("Longitude"));
				}
				location.appendChild(document.createTextNode(item.getString("Address")));
				root.appendChild(location);
				Element country = document.createElement("Country");
				country.appendChild(document.createTextNode(item.getString("Country")));
				root.appendChild(country);

				Date date = startingFormat.parse(item.getString("StartTime"));;
				Element started = document.createElement("Started");
				started.appendChild(document.createTextNode(endFormat.format(date)));
				root.appendChild(started);
				date = startingFormat.parse(item.getString("EndTime"));
				Element ends = document.createElement("Ends");
				ends.appendChild(document.createTextNode(endFormat.format(date)));
				root.appendChild(ends);
				Statement sellerStatement = conn.createStatement();
				ResultSet sellerSet = sellerStatement.executeQuery("SELECT * FROM Items, Users WHERE Items.UserID = Users.UserID AND Items.ItemID = " + itemId);
				if (sellerSet.next())
				{
					Element seller = document.createElement("Seller");
					seller.setAttribute("SellerRating", sellerSet.getString("SellerRating"));
					seller.setAttribute("UserID", sellerSet.getString("UserID"));
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
