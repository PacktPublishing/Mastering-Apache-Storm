package com.stormadvance.logprocessing;

import java.sql.Connection;
import java.sql.PreparedStatement;

import org.apache.storm.tuple.Tuple;
/**
 * This class contains logic to persist record into MySQL database.
 * 
 */
public class MySQLDump {
	/**
	 * Name of database you want to connect
	 */
	private String database;
	/**
	 * Name of MySQL user
	 */
	private String user;
	/**
	 * IP of MySQL server
	 */
	private String ip;
	/**
	 * Password of MySQL server
	 */
	private String password;
	
	public MySQLDump(String ip, String database, String user, String password) {
		this.ip = ip;
		this.database = database;
		this.user = user;
		this.password = password;
	}
	
	/**
	 * Get the MySQL connection
	 */
	private Connection connect = MySQLConnection.getMySQLConnection(ip,database,user,password);

	private PreparedStatement preparedStatement = null;
	
	/**
	 * Persist input tuple.
	 * @param tuple
	 */
	public void persistRecord(Tuple tuple) {
		try {

			// preparedStatements can use variables and are more efficient
			preparedStatement = connect
					.prepareStatement("insert into  apachelog values (default,?, ?, ?, ?, ? , ?, ?, ?,?,?,?)");

			preparedStatement.setString(1, tuple.getStringByField("ip"));
			preparedStatement.setString(2, tuple.getStringByField("dateTime"));
			preparedStatement.setString(3, tuple.getStringByField("request"));
			preparedStatement.setString(4, tuple.getStringByField("response"));
			preparedStatement.setString(5, tuple.getStringByField("bytesSent"));
			preparedStatement.setString(6, tuple.getStringByField("referrer"));
			preparedStatement.setString(7, tuple.getStringByField("useragent"));
			preparedStatement.setString(8, tuple.getStringByField("country"));
			preparedStatement.setString(9, tuple.getStringByField("browser"));
			preparedStatement.setString(10, tuple.getStringByField("os"));
			preparedStatement.setString(11, tuple.getStringByField("keyword"));
			
			// Insert record
			preparedStatement.executeUpdate();

		} catch (Exception e) {
			throw new RuntimeException(
					"Error occure while persisting records in mysql : ");
		} finally {
			// close prepared statement
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (Exception exception) {
					System.out
							.println("Error occure while closing PreparedStatement : ");
				}
			}
		}

	}
	
	public void close() {
		try {
		connect.close();
		}catch(Exception exception) {
			System.out.println("Error occure while clossing the connection");
		}
	}
	
	
}
