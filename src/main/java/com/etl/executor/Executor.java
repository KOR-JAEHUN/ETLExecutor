package com.etl.executor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 2019-04-26
 * Hadoop에서 데이터를 READ한 후 Neo4j에 테이블을 만들어주는 메소드를 실행하는 클래스
 * @author song
 *
 */
public class Executor {
	
	private static final Logger logger = LoggerFactory.getLogger(Executor.class);
	
	public static void main(String[] args) {
//		ConfigurableApplicationContext ctx = new GenericXmlApplicationContext();
//        final ConfigurableEnvironment env = ctx.getEnvironment();
//        MutablePropertySources propertySources = env.getPropertySources();
//        
//        try {
//            propertySources.addLast(new ResourcePropertySource("classpath:db.properties"));
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
        
        long startTime = System.currentTimeMillis();

        Executor exe = new Executor();
//        exe.copyTableOdsToLake();
        exe.readHadoop();
        
        long endTime = System.currentTimeMillis();
        long time = endTime - startTime;
        System.out.println("실행시간 =============== " + time/1000.0 + "초");
	}
	
	public void readHadoop() {
		
        ResultSet rs = null;
        Connection conn = null;
        Connection tconn = null;
		try{
	        System.out.println("######### 적재 시작 ##########");
	        tconn = DBConnByTibero.getInstacne().getConnection();
			conn = DBConnByHadoop.getInstacne().getConnection();
	        
	        Statement stmt = conn.createStatement();
	        
	        String sql = "select * from big_unity.etl_table_list where type = 'test'";
	        
	        rs = stmt.executeQuery(sql);
	        
	        while(rs.next()){
	        	try {
	        		String tableNm = rs.getString("table_nm");
	        		String dbNm = rs.getString("db_nm");
	        		ReadHadoopData hd = new ReadHadoopData();
	        		System.out.println("적재 테이블 === " + dbNm + "." + tableNm);
	        		hd.readHadoopData(tableNm, dbNm, conn, tconn);
				} catch (Exception e) {
					throw new Exception(e);
				}
	        }
	        
	        rs.close();
	        conn.close();
	        tconn.close();
	        System.out.println("######### 적재 종료 ##########");
	        
	    }catch(Exception ex){
	    	logger.error(ex.getMessage());
			System.out.println("######### 적재 실패 ##########");
	        ex.printStackTrace();
	    }finally {
	        try{
	            if( rs != null ){
	                rs.close();                
	            }
	        }catch(Exception ex){
	            rs = null;
	        }
	        
	        try{
	            if( conn != null ){
	                conn.close();                
	            }
	        }catch(Exception ex){
	            conn = null;
	        }
	        
	        try{
	        	if( tconn != null ){
	        		tconn.close();                
	        	}
	        }catch(Exception ex){
	        	tconn = null;
	        }
	    }
	}

	// ODS에서 LAKE로 테이블 복제 
	public void copyTableOdsToLake() {
			
        ResultSet rs = null;
        Connection conn = null;
		try{
			System.out.println("######### ODS TO LAKE ##########");
	        System.out.println("######### 테이블 복제 시작 ##########");
			conn = DBConnByHadoop.getInstacne().getConnection();
	        
	        Statement stmt = conn.createStatement();
	        
	        String sql = "select * from big_unity.etl_table_list where type = 'test'";
	        
	        rs = stmt.executeQuery(sql);
	        
	        while(rs.next()){
	        	try {
	        		String tableNm = rs.getString("table_nm");
	        		String dbNm = rs.getString("db_nm");
	        		CopyHadoopLake lake = new CopyHadoopLake();
	        		System.out.println("복제 테이블 === " + dbNm + "." + tableNm);
	        		lake.copyLake(tableNm, dbNm, conn);
				} catch (Exception e) {
					throw new Exception(e);
				}
	        }
	        
	        rs.close();
	        conn.close();
	        System.out.println("######### 테이블 복제 성공 ##########");
	        
	    }catch(Exception ex){
	    	logger.error(ex.getMessage());
			System.out.println("######### 테이블 복제 실패 ##########");
	        ex.printStackTrace();
	    }finally {
	        try{
	            if( rs != null ){
	                rs.close();                
	            }
	        }catch(Exception ex){
	            rs = null;
	        }
	        
	        try{
	            if( conn != null ){
	                conn.close();                
	            }
	        }catch(Exception ex){
	            conn = null;
	        }
	        
	    }
	}

}
