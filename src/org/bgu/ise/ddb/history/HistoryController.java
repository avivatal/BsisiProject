/**
 * 
 */
package org.bgu.ise.ddb.history;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.MediaItems;
import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.bgu.ise.ddb.items.ItemsController;
import org.bgu.ise.ddb.registration.RegistarationController;
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
import com.mongodb.client.model.Filters;

/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/history")
public class HistoryController extends ParentController{

	public static final String localhost = "localhost";
	public static final int port = 27017;

	/**
	 * The function inserts to the system storage triple(s)(username, title, timestamp). 
	 * The timestamp - in ms since 1970
	 * Advice: better to insert the history into two structures( tables) in order to extract it fast one with the key - username, another with the key - title
	 * @param username
	 * @param title
	 * @param response
	 */
	@RequestMapping(value = "insert_to_history", method={RequestMethod.GET})
	public void insertToHistory (@RequestParam("username")    String username,
			@RequestParam("title")   String title,
			HttpServletResponse response){
		MongoClient client = null;
		HttpStatus status = null;
		try {
			RegistarationController rgCtrl = new RegistarationController();
			ItemsController itemsCtrl = new ItemsController();

			if(rgCtrl.isExistUser(username) && itemsCtrl.isExistMovie(title)) {
				client = new MongoClient(localhost,port);
				MongoCollection table = client.getDatabase("local").getCollection("History");
				Document history = new Document();
				history.append("Username", username);
				history.append("Title", title);
				history.append("Timestamp", new Date().getTime());
				table.insertOne(history);
				status = HttpStatus.OK;
			} else {
				status = HttpStatus.BAD_REQUEST;
			}
			response.setStatus(status.value());
		} catch(Exception e) {
			e.printStackTrace();
		}
		finally {
			client.close();
		}
		response.setStatus(status.value());
	}



	/**
	 * The function retrieves  users' history
	 * The function return array of pairs <title,viewtime> sorted by VIEWTIME in descending order
	 * @param username
	 * @return
	 */
	@RequestMapping(value = "get_history_by_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByUser(@RequestParam("entity")    String username){
		ArrayList<HistoryPair> histPairs = new ArrayList();
		MongoClient client = null;
		try {
			RegistarationController rgCtrl = new RegistarationController();
			if (rgCtrl.isExistUser(username)) {
				client = new MongoClient(localhost, port);
				MongoCollection table = client.getDatabase("local").getCollection("History");			
				MongoCursor userHistory = table.find(Filters.eq("Username", username)).iterator();
				while(userHistory.hasNext()) {
					Document pair = (Document)userHistory.next();
					histPairs.add(new HistoryPair(pair.get("Title").toString(), new Date(Long.parseLong(pair.get("Timestamp").toString(), 10))));
				}
			}

		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			client.close();
		}
		return histPairs.toArray(new HistoryPair[histPairs.size()]);
	}


	/**
	 * The function retrieves  items' history
	 * The function return array of pairs <username,viewtime> sorted by VIEWTIME in descending order
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_history_by_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByItems(@RequestParam("entity")    String title){
		ArrayList<HistoryPair> histPairs = new ArrayList();
		MongoClient client = null;
		try {
			ItemsController itemCtrl = new ItemsController();
			if (itemCtrl.isExistMovie(title)) {
				client = new MongoClient(localhost, port);
				MongoCollection table = client.getDatabase("local").getCollection("History");			
				MongoCursor titleHistory = table.find(Filters.eq("Title", title)).iterator();
				while(titleHistory.hasNext()) {
					Document pair = (Document)titleHistory.next();
					histPairs.add(new HistoryPair(pair.get("Username").toString(), new Date(Long.parseLong(pair.get("Timestamp").toString(), 10))));
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			client.close();
		}
		return histPairs.toArray(new HistoryPair[histPairs.size()]);
	}

	/**
	 * The function retrieves all the  users that have viewed the given item
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_users_by_item",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  User[] getUsersByItem(@RequestParam("title") String title){
		Set<User> usersByItem = new HashSet();
		MongoClient client = null;
		try {
			ItemsController itemCtrl = new ItemsController();
			if (itemCtrl.isExistMovie(title)) {
				client = new MongoClient(localhost, port);
				MongoCollection table = client.getDatabase("local").getCollection("History");			
				MongoCollection usersTable = client.getDatabase("local").getCollection("Users");
				MongoCursor titleHistory = table.find(Filters.eq("Title", title)).iterator();
				while(titleHistory.hasNext()) {
					Document pair = (Document)titleHistory.next();
					MongoCursor usersIterator = usersTable.find(Filters.eq("Username", pair.get("Username"))).iterator();
					Document user = (Document) usersIterator.next();
					usersByItem.add(new User(user.get("Username").toString(),  user.get("Password").toString(), user.get("Firstname").toString(), user.get("Lastname").toString()));
				}
			}

		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			client.close();
		}
		return usersByItem.toArray(new User[usersByItem.size()]);
	}

	/**
	 * The function calculates the similarity score using Jaccard similarity function:
	 *  sim(i,j) = |U(i) intersection U(j)|/|U(i) union U(j)|,
	 *  where U(i) is the set of usernames which exist in the history of the item i.
	 * @param title1
	 * @param title2
	 * @return
	 */
	@RequestMapping(value = "get_items_similarity",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	public double  getItemsSimilarity(@RequestParam("title1") String title1,
			@RequestParam("title2") String title2){

		ItemsController itemCtrl = new ItemsController();
		try {
			if (itemCtrl.isExistMovie(title1) && itemCtrl.isExistMovie(title2)) {
				User[] title1Users = getUsersByItem(title1);
				User[] title2Users = getUsersByItem(title2);
				int counter = 0;
				for (User user1 : Arrays.asList(title1Users)) {
					if (Arrays.asList(title2Users).stream().anyMatch(user2 -> user2.getUsername().equals(user1.getUsername()))) {
						counter++;
					}
				}
				double intersectionNumber = counter;

				List<User> list1 = Arrays.asList(title1Users);
				List<User> list2 = Arrays.asList(title2Users);

				Set<String> unionSet = new HashSet<String>();
				list1.stream().forEach(list1User -> unionSet.add(list1User.getUsername()));
				list2.stream().forEach(list2User -> unionSet.add(list2User.getUsername()));

				double union = unionSet.size();

				return Math.max(0, intersectionNumber / union);
			} else {
				return 0;
			}
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
	}
}

