package com.etl.executor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.io.support.ResourcePropertySource;

public class DBConnByHadoop {

	private static DBConnByHadoop hJdbc;
	Connection conn = null;
	
	private DBConnByHadoop() {}
	
	public static DBConnByHadoop getInstacne() {
		if (hJdbc == null) {
			hJdbc = new DBConnByHadoop(); 
		}
		return hJdbc;
	}
	
	public Connection getConnection() throws ClassNotFoundException, SQLException {
		if(conn == null || conn.isClosed()) {
			ConfigurableApplicationContext ctx = new GenericXmlApplicationContext();
			final ConfigurableEnvironment env = ctx.getEnvironment();
			MutablePropertySources propertySources = env.getPropertySources();
			    
			try {
				propertySources.addLast(new ResourcePropertySource("classpath:db.properties"));
			} catch (IOException e) {
			// TODO Auto-generated catch block
				e.printStackTrace();
			}  
			
			String driver = env.getProperty("hive.driver");
	        Class.forName(driver);
	        
	        String hive_url = env.getProperty("hive.url");
	        String hive_username = env.getProperty("hive.username");
	        String hive_pw = env.getProperty("hive.password");
	        
	        conn = DriverManager.getConnection(hive_url, hive_username, hive_pw);
		}
		
		return conn;
	}

}
