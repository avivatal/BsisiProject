/**
 * 
 */
package org.bgu.ise.ddb.items;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.MediaItems;
import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.bson.Document;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;



/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/items")
public class ItemsController extends ParentController {
	
	public static final String localhost = "localhost";
	public static final int port = 27017;
    Connection conn = null;
    private final static String driver = "oracle.jdbc.driver.OracleDriver";
    private String connectionUrl = "jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521/ORACLE";
    private String username = "talshemt";
    private String password = "talShem3";
    
    private void connect() {
        try {
        	Class.forName(driver);
            conn = DriverManager.getConnection(connectionUrl, username, password);
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch(ClassNotFoundException e) {
        	e.printStackTrace();
        }
    }
	/**
	 * The function copy all the items(title and production year) from the Oracle table MediaItems to the System storage.
	 * The Oracle table and data should be used from the previous assignment
	 */
	@RequestMapping(value = "fill_media_items", method={RequestMethod.GET})
	public void fillMediaItems(HttpServletResponse response){
		HttpStatus status = HttpStatus.OK;
		MongoClient client = null;
		ResultSet rs = null;
		try {
			
	        if (conn == null || conn.isClosed()) {
	            connect();
	        }
			client = new MongoClient(localhost,port);
			MongoCollection table = client.getDatabase("local").getCollection("MediaItems");
			
	        String query = "SELECT * FROM MEDIAITEMS";
            PreparedStatement ps = conn.prepareStatement(query);
            rs = ps.executeQuery();            
            while (rs.next()) {            	
				Document movie = new Document();
				movie.append("Title", rs.getString("TITLE"));
				movie.append("Prod_Year", rs.getString("PROD_YEAR"));				
				table.insertOne(movie);
            }
            rs.close();
            conn.close();
			
		} catch(Exception e) {
			e.printStackTrace();
			
		} finally {
            client.close();
		}
		
		response.setStatus(status.value());
	}
	
	

	/**
	 * The function copy all the items from the remote file,
	 * the remote file have the same structure as the films file from the previous assignment.
	 * You can assume that the address protocol is http
	 * @throws IOException 
	 */
	@RequestMapping(value = "fill_media_items_from_url", method={RequestMethod.GET})
	public void fillMediaItemsFromUrl(@RequestParam("url")    String urladdress,
			HttpServletResponse response) throws IOException{
		System.out.println(urladdress);
		HttpStatus status = HttpStatus.OK;
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
		MongoClient client = null;
        
		try {
			client = new MongoClient(localhost,port);
			MongoCollection table = client.getDatabase("local").getCollection("MediaItems");
			br = new BufferedReader(new InputStreamReader(new URL(urladdress).openStream()));
            //reads each line and inserts new record
            while ((line = br.readLine()) != null) {
                String[] movie = line.split(cvsSplitBy);
                movie[0] = movie[0].replace("'", "''");
                
                Document movieDoc = new Document();
                movieDoc.append("Title", movie[0]);
                movieDoc.append("Prod_Year", movie[1]);
                table.insertOne(movieDoc);
            } 
            status = HttpStatus.OK;
		} catch(Exception e) {
			e.printStackTrace();
			status = HttpStatus.BAD_REQUEST;
		} finally {
			client.close();
		}
		
		response.setStatus(status.value());
	}
	
	
	/**
	 * The function retrieves from the system storage N items,
	 * order is not important( any N items) 
	 * @param topN - how many items to retrieve
	 * @return
	 */
	@RequestMapping(value = "get_topn_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(MediaItems.class)
	public  MediaItems[] getTopNItems(@RequestParam("topn")    int topN){	
		ArrayList<MediaItems> movies = new ArrayList();
		MongoClient client = null;
		int counter = 0;
		try {
			if (topN > 0) {
				client = new MongoClient(localhost, port);
				MongoCollection table = client.getDatabase("local").getCollection("MediaItems");			
				MongoCursor allmovies = table.find().iterator();
				while(allmovies.hasNext() && counter<topN) {
					Document user = (Document)allmovies.next();
					movies.add(new MediaItems(user.get("Title").toString(),Integer.parseInt(user.get("Prod_Year").toString())));
					counter++;
				}
			}

		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			client.close();
		}
		return movies.toArray(new MediaItems[movies.size()]);
	}
	
	/**
	 * The function returns true if the received title exist in the system otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "is_exist_movie", method={RequestMethod.GET})
	public boolean isExistMovie(@RequestParam("title") String title) throws IOException{
		boolean result = false;
		MongoClient client = null;

		try {
			client = new MongoClient(localhost, port);
			MongoCollection table = client.getDatabase("local").getCollection("MediaItems");
			Document res = new Document();
			res.append("Title", title);
			MongoCursor iterator = table.find(res).iterator();

			while(iterator.hasNext()) {
				result = true;
				break;
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			client.close();
		}
		return result;

	}
		

}
