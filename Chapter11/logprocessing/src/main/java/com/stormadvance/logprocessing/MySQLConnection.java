package com.stormadvance.logprocessing;

import java.sql.Connection;
import java.sql.DriverManager;
/**
 * 
 * This class return the MySQL connection.
 */
public class MySQLConnection {

	private static Connection connect = null;

	/**
	 * This method return the MySQL connection.
	 * 
	 * @param ip
	 *            ip of MySQL server
	 * @param database
	 *            name of database
	 * @param user
	 *            name of user
	 * @param password
	 *            password of given user
	 * @return MySQL connection
	 */
	public static Connection getMySQLConnection(String ip, String database, String user, String password) {
		try {
			// this will load the MySQL driver, each DB has its own driver
			Class.forName("com.mysql.jdbc.Driver");
			// setup the connection with the DB.
			connect = DriverManager
					.getConnection("jdbc:mysql://"+ip+"/"+database+"?"
							+ "user="+user+"&password="+password+"");
			return connect;
		} catch (Exception e) {
			throw new RuntimeException("Error occure while get mysql connection : ");
		}
	}
}
