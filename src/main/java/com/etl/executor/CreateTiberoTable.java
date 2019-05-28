package com.etl.executor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 기존 테이블을 삭제 한 후 미리 만들어둔 스키마 테이블을 사용하여 새로운 테이블을 만든다.
 * 데이터를 delete한 후 insert하는것에 대한 티베로 성능 이슈때문에 DROP해버림!(티베로 ㅂㄷㅂㄷ)
 * @author SONGJAEHUN
 *
 */
public class CreateTiberoTable {
	
	
	Statement stmt = null;
	ResultSet rs = null;
	public void copyTable(String tableNm, Connection conn) throws Exception {
		try {
			/*
			 * ctas로 하면 index, pk, fk가 복사 되지 않기때문에 truncate로 변경 
			 * 
//			String sql ="SELECT count(*) as cnt FROM USER_TABLES WHERE TABLE_NAME = '" + tableNm + "'";
//			String sql ="SELECT count(*) as cnt FROM USER_TABLES WHERE TABLE_NAME = '" + tableNm + "_SCHEMA'";
			String sql ="SELECT count(*) as cnt FROM USER_TABLES WHERE TABLE_NAME = '" + tableNm + "_BAK'";
			System.out.println(sql);
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			rs.next();
			int cnt = rs.getInt("cnt");
			if(cnt > 0) {
//				sql ="DROP TABLE " + tableNm;
//				sql ="DROP TABLE " + tableNm + "_SCHEMA";
				sql ="DROP TABLE " + tableNm + "_BAK";
				System.out.println(sql);
				stmt = conn.createStatement();
				stmt.execute(sql);
			}
//			sql = "CREATE TABLE " + tableNm + " AS SELECT * FROM  " + tableNm + "_SCHEMA " + " where 1=2"; // 스키마만 복제
//			sql = "CREATE TABLE " + tableNm + "_SCHEMA " + " AS SELECT * FROM  " + tableNm + " where 1=2"; // 스키마만 복제
			sql = "CREATE TABLE " + tableNm + "_BAK " + " AS SELECT * FROM  " + tableNm + " where 1=2"; // 스키마만 복제
			System.out.println(sql);
			stmt = conn.createStatement();
			stmt.execute(sql);
			*/
			
			//String sql ="DROP TABLE " + tableNm;
			//String sql ="DROP TABLE " + tableNm + "_SCHEMA";
			String sql ="TRUNCATE TABLE " + tableNm;
			System.out.println(sql);
			stmt = conn.createStatement();
			stmt.execute(sql);
			
	        
		} catch (SQLException e) {
			throw new Exception(e);
		} finally {
			try{
	        	if( stmt != null ){
	        		stmt.close();                
	        	}
	        }catch(Exception ex){
	        	stmt = null;
	        }
			try{
				if( rs != null ){
					rs.close();                
				}
			}catch(Exception ex){
				rs = null;
			}
		}
	}
	
	// 증분 임시테이블
	public void copyTmpTable(String tableNm, Connection conn) throws Exception {
		try {
			/*
			 * ctas로 하면 index, pk, fk가 복사 되지 않기때문에 truncate로 변경 
			 * */
//			String sql ="SELECT count(*) as cnt FROM USER_TABLES WHERE TABLE_NAME = '" + tableNm + "'";
//			String sql ="SELECT count(*) as cnt FROM USER_TABLES WHERE TABLE_NAME = '" + tableNm + "_SCHEMA'";
			String sql ="SELECT count(*) as cnt FROM USER_TABLES WHERE TABLE_NAME = '" + tableNm + "_TMP'";
			System.out.println(sql);
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			rs.next();
			int cnt = rs.getInt("cnt");
			if(cnt > 0) {
//				sql ="DROP TABLE " + tableNm;
//				sql ="DROP TABLE " + tableNm + "_SCHEMA";
				sql ="DROP TABLE " + tableNm + "_TMP";
				System.out.println(sql);
				stmt = conn.createStatement();
				stmt.execute(sql);
			}
//			sql = "CREATE TABLE " + tableNm + " AS SELECT * FROM  " + tableNm + "_SCHEMA " + " where 1=2"; // 스키마만 복제
//			sql = "CREATE TABLE " + tableNm + "_SCHEMA " + " AS SELECT * FROM  " + tableNm + " where 1=2"; // 스키마만 복제
			sql = "CREATE TABLE " + tableNm + "_TMP " + " AS SELECT * FROM  " + tableNm + " where 1=2"; // 스키마만 복제
			System.out.println(sql);
			stmt = conn.createStatement();
			stmt.execute(sql);
			
			
//			//String sql ="DROP TABLE " + tableNm;
//			//String sql ="DROP TABLE " + tableNm + "_SCHEMA";
//			String sql ="TRUNCATE TABLE " + tableNm + "_BAK";
//			System.out.println(sql);
//			stmt = conn.createStatement();
//			stmt.execute(sql);
			
			
		} catch (SQLException e) {
			throw new Exception(e);
		} finally {
			try{
				if( stmt != null ){
					stmt.close();                
				}
			}catch(Exception ex){
				stmt = null;
			}
			try{
				if( rs != null ){
					rs.close();                
				}
			}catch(Exception ex){
				rs = null;
			}
		}
	}
	
	// 삭제를 위한 삭제임시테이블
	public void copyDelTable(String tableNm, Connection conn) throws Exception {
		try {
			/*
			 * ctas로 하면 index, pk, fk가 복사 되지 않기때문에 truncate로 변경 
			 * */
			String sql ="SELECT count(*) as cnt FROM USER_TABLES WHERE TABLE_NAME = '" + tableNm + "_DEL'";
			System.out.println(sql);
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			rs.next();
			int cnt = rs.getInt("cnt");
			if(cnt > 0) {
				sql ="DROP TABLE " + tableNm + "_DEL";
				System.out.println(sql);
				stmt = conn.createStatement();
				stmt.execute(sql);
			}
			sql = "CREATE TABLE " + tableNm + "_DEL " + " AS SELECT * FROM  " + tableNm + " where 1=2"; // 스키마만 복제
			System.out.println(sql);
			stmt = conn.createStatement();
			stmt.execute(sql);
			
//			//String sql ="DROP TABLE " + tableNm;
//			//String sql ="DROP TABLE " + tableNm + "_SCHEMA";
//			String sql ="TRUNCATE TABLE " + tableNm + "_BAK";
//			System.out.println(sql);
//			stmt = conn.createStatement();
//			stmt.execute(sql);
			
			
		} catch (SQLException e) {
			throw new Exception(e);
		} finally {
			try{
				if( stmt != null ){
					stmt.close();                
				}
			}catch(Exception ex){
				stmt = null;
			}
			try{
				if( rs != null ){
					rs.close();                
				}
			}catch(Exception ex){
				rs = null;
			}
		}
	}
}
