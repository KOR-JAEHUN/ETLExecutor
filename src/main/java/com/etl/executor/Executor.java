package com.etl.executor;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;

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
	
	// 삭제를 위한 테이블
//	private String[] delete_data_table = {"TAC_ACP_SBJT", "TEV_EVLR_SBJT_ESTM", "TSL_SLC_SBJT_RSLT", "TAC_KWD", "TCM_FILE_DTL", "TAC_SBJT_TPI_HR", "TAC_SMMR_CNTN", "TRT_PPR", "TEV_PN_EVLR_ESTM_SBJT",
//										 "KCDM312", "KCDM310"};
	private String[] delete_data_table = {};
	
	public static void main(String[] args) {
        
        long startTime = System.currentTimeMillis();

        Executor exe = new Executor();
//        exe.copyTableOdsToLake();
        exe.readHadoop();
        
        long endTime = System.currentTimeMillis();
        long time = endTime - startTime;
        System.out.println("총 실행시간 =============== " + time/1000.0 + "초");
	}
	
	public void readHadoop() {
		InsertLog log = new InsertLog();
        ResultSet rs = null;
        Connection conn = null;
        Connection tconn = null;
        Statement stmt = null;
		try{
			InetAddress local = InetAddress.getLocalHost();
			String ip = local.getHostAddress();
//			System.out.println("server ip = " + ip);
	        System.out.println("######### 적재 시작 ##########");
	        tconn = DBConnByTibero.getInstacne().getConnection();
			conn = DBConnByHadoop.getInstacne().getConnection();
	        
			stmt = conn.createStatement();
	        
//	        String sql = "select * from ods_batch_hist.etl_table_list";
//	        String sql = "select * from ods_batch_hist.etl_table_list where table_nm in('KCDD101')";
	        String sql = "";
	        if("100.100.100.216".contains(ip)) {
	        	sql = "select * from ods_batch_hist.etl_table_list where db_nm = 'ods_ernd'";
	        	System.out.println("ERND START");
	        }else if("100.100.100.217".contains(ip)) {
	        	sql = "select * from ods_batch_hist.etl_table_list where db_nm = 'ods_kci'";
	        	System.out.println("KCI START");
	        }else if("100.100.100.218".contains(ip)) {
		        sql = "select * from ods_batch_hist.etl_table_list where db_nm = 'ods_kri'";
		        System.out.println("KRI START");
	        }
	        
	        rs = stmt.executeQuery(sql);
	        
	        Calendar cal = Calendar.getInstance();
	        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	        String startDt = "";
	        String endDt = "";
	        String tableNm = "";
	        String dbNm = "";
	        while(rs.next()){
	        	try {
					startDt = sdf.format(cal.getTime());
	        		long startTime = System.currentTimeMillis();
	        		
	        		tableNm = rs.getString("table_nm");
	        		dbNm = rs.getString("db_nm");
	        		ReadHadoopData hd = new ReadHadoopData();
	        		System.out.println("적재 테이블 === " + dbNm + "." + tableNm);
	        		if(Arrays.asList(delete_data_table).contains(tableNm)) {
	        			hd.readAndDeleteHadoopData(tableNm, dbNm, conn, tconn);
	        		}
	        		int cnt = hd.readAndInsertHadoopData(tableNm, dbNm, conn, tconn);
	        		
	        		endDt = sdf.format(cal.getTime());
	        		long endTime = System.currentTimeMillis();
	        		long time = endTime - startTime;
	        		log.insertLog(dbNm, tableNm, conn, startDt, endDt, "SUCCESS", cnt);
	        		System.out.println(tableNm + " 테이블 실행시간 =============== " + time/1000.0 + "초");
				} catch (Exception e) {
					log.insertLog(dbNm, tableNm, conn, startDt, endDt, "FAIL", '-');
				}
	        }
	        
	        rs.close();
	        stmt.close();
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
	        	if( stmt != null ){
	        		stmt.close();                
	        	}
	        }catch(Exception ex){
	        	stmt = null;
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
        Statement stmt = null;
		try{
			System.out.println("######### ODS TO LAKE ##########");
	        System.out.println("######### 테이블 복제 시작 ##########");
			conn = DBConnByHadoop.getInstacne().getConnection();
	        
			stmt = conn.createStatement();
	        
	        String sql = "select * from ods_batch_hist.etl_table_list";
	        
	        rs = stmt.executeQuery(sql);
	        
	        while(rs.next()){
        		String tableNm = rs.getString("table_nm");
        		String dbNm = rs.getString("db_nm");
        		CopyHadoopLake lake = new CopyHadoopLake();
        		System.out.println("복제 테이블 === " + dbNm + "." + tableNm);
        		lake.copyLake(tableNm, dbNm, conn);
	        }
	        rs.close();
	        stmt.close();
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
	        	if( stmt != null ){
	        		stmt.close();                
	        	}
	        }catch(Exception ex){
	        	stmt = null;
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
