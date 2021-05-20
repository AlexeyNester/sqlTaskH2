package sqlTask.sqlTask;

import java.io.File;
import java.io.PrintWriter;
import java.sql.*;

public class dbConverter { 

	   static final String JDBC_DRIVER = "org.h2.Driver";   
	   static final String DB_URL = "jdbc:h2:tcp://localhost/~/test";  
	   

	   static final String USER = "sa"; 
	   static final String PASS = ""; 

	public static void main(String[] args) {
		Connection conn = null; 
	    Statement stmt = null; 
	    try { 
	    	
	        Class.forName(JDBC_DRIVER); // Register JDBC driver     
	        System.out.println("Connecting to database..."); 
	        conn = DriverManager.getConnection(DB_URL,USER,PASS); //Open a connection  
	         
	        //Execute a query 
	        System.out.println("Reading table in given database..."); 
	        stmt = conn.createStatement(); 
	        String sql =  "SELECT TABLE_COLS.TABLE_NAME, COLUMN_NAME, COLUMN_TYPE FROM TABLE_COLS, TABLE_LIST "
	        		+ "WHERE (TABLE_COLS.TABLE_NAME = TABLE_LIST.TABLE_NAME "
	        		+ "AND LOWER(TABLE_COLS.COLUMN_NAME) = LOWER(TABLE_LIST.PK))"; //LOWER allow to create case insensitive query 
	        ResultSet result = stmt.executeQuery(sql);
	        File file = new File("TableName ColumnName PK.txt"); // Create new file
	        PrintWriter pw = new PrintWriter(file);
	        if (result != null) {
	        	System.out.println("Writing required columns in the file");
		        while(result.next()) {
		            String tableName = result.getString("TABLE_NAME");
		            String columnName  = result.getString("COLUMN_NAME");
		            String columnType = result.getString("COLUMN_TYPE");
		            
		            pw.println(tableName + " " + columnName + " " + columnType); // Write column in txt file
		        }
	        }
	         
	        //Clean-up environment 
	        pw.close();
	        stmt.close(); 
	        conn.close(); 
	        result.close();
	      } catch(SQLException se) { 
	         //Handle errors for JDBC 
	         se.printStackTrace(); 
	      } catch(Exception e) { 
	         //Handle errors for Class.forName 
	         e.printStackTrace(); 
	      } finally { 
	         //finally block used to close resources 
	         try{ 
	            if(stmt!=null) stmt.close(); 
	         } catch(SQLException se2) { 
	         } // nothing we can do 
	         try { 
	            if(conn!=null) conn.close(); 
	         } catch(SQLException se){ 
	            se.printStackTrace(); 
	         } //end finally try 
	      } //end try 
	      System.out.println("That's all");
	   } 
	}
