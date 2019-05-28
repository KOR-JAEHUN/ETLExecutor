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

public class DBConnByTibero {

	private static DBConnByTibero tJdbc;
	Connection conn = null;
	
	private DBConnByTibero() {}
	
	public static DBConnByTibero getInstacne() {
		if (tJdbc == null) {
			tJdbc = new DBConnByTibero(); 
		}
		return tJdbc;
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
			
			String driver = env.getProperty("tibero.driver");
	        Class.forName(driver);
	        
	        String tibero_url = env.getProperty("tibero.url");
	        String tibero_username = env.getProperty("tibero.username");
	        String tibero_pw = env.getProperty("tibero.password");
	        
	        conn = DriverManager.getConnection(tibero_url, tibero_username, tibero_pw);
		}
		
		return conn;
	}

}
