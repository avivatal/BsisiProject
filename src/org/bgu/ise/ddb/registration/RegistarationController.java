/**
 * 
 */
package org.bgu.ise.ddb.registration;
import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;


import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Alex
 *
 */
@RestController
@RequestMapping(value = "/registration")
public class RegistarationController extends ParentController{

	public static final String localhost = "localhost";
	public static final int port = 27017;


	/**
	 * The function checks if the username exist,
	 * in case of positive answer HttpStatus in HttpServletResponse should be set to HttpStatus.CONFLICT,
	 * else insert the user to the system  and set to HttpStatus in HttpServletResponse HttpStatus.OK
	 * @param username
	 * @param password
	 * @param firstName
	 * @param lastName
	 * @param response
	 */
	@RequestMapping(value = "register_new_customer", method={RequestMethod.POST})
	public void registerNewUser(@RequestParam("username") String username,
			@RequestParam("password")    String password,
			@RequestParam("firstName")   String firstName,
			@RequestParam("lastName")  String lastName,
			HttpServletResponse response){
		System.out.println(username+" "+password+" "+lastName+" "+firstName);
		MongoClient client = null;
		try {
			HttpStatus status = HttpStatus.BAD_REQUEST;
			if(isExistUser(username)) {
				System.out.println("false");
				status = HttpStatus.CONFLICT;
			} else {
				System.out.println("true");
				client = new MongoClient(localhost,port);
				MongoCollection table = client.getDatabase("local").getCollection("Users");

				Document user = new Document();
				user.append("Username", username);
				user.append("Password", password);
				user.append("Firstname", firstName);
				user.append("Lastname", lastName);
				user.append("Date", Instant.now());
				table.insertOne(user);
				status = HttpStatus.OK;
			}
			response.setStatus(status.value());
		} catch(Exception e) {
			e.printStackTrace();
		}
		finally {
			client.close();
		}
	}

	/**
	 * The function returns true if the received username exist in the system otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "is_exist_user", method={RequestMethod.GET})
	public boolean isExistUser(@RequestParam("username") String username) throws IOException{
		System.out.println(username);
		boolean result = false;
		MongoClient client = null;

		try {
			client = new MongoClient(localhost, port);
			MongoCollection table = client.getDatabase("local").getCollection("Users");
			Document res = new Document();
			res.append("Username", username);
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

	/**
	 * The function returns true if the received username and password match a system storage entry, otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "validate_user", method={RequestMethod.POST})
	public boolean validateUser(@RequestParam("username") String username,
			@RequestParam("password")    String password) throws IOException{
		System.out.println(username+" "+password);
		boolean result = false;
		MongoClient client = null;

		try {
			client = new MongoClient(localhost, port);
			MongoCollection table = client.getDatabase("local").getCollection("Users");
			Document res = new Document();
			res.append("Username", username);
			res.append("Password", password);
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

	/**
	 * The function retrieves number of the registered users in the past n days
	 * @param days
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "get_number_of_registred_users", method={RequestMethod.GET})
	public int getNumberOfRegistredUsers(@RequestParam("days") int days) throws IOException{
		System.out.println(days+"");
		int result = 0;

		MongoClient client = null;
		try {
			if (days > 0) {
				client = new MongoClient(localhost, port);
				MongoCollection table = client.getDatabase("local").getCollection("Users");			
				MongoCursor allUsersInLastNdays = table.find(Filters.gt("Date", Instant.now().minus(days,ChronoUnit.DAYS))).iterator();
				while(allUsersInLastNdays.hasNext()) {
					result++;
					allUsersInLastNdays.next();
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			client.close();
		}

		return result;

	}

	/**
	 * The function retrieves all the users
	 * @return
	 */
	@RequestMapping(value = "get_all_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(User.class)
	public  User[] getAllUsers(){

		ArrayList<User> users = new ArrayList();
		MongoClient client = null;

		try {

			client = new MongoClient(localhost, port);
			MongoCollection table = client.getDatabase("local").getCollection("Users");			
			MongoCursor allUsers = table.find().iterator();
			while(allUsers.hasNext()) {
				Document user = (Document)allUsers.next();
				users.add(new User(user.get("Username").toString(),user.get("Password").toString(),user.get("Firstname").toString(),user.get("Lastname").toString()));
			}

		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			client.close();
		}
		return users.toArray(new User[users.size()]);
	}

}
