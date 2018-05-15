package com.ivan.persistence;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;

public class DBSupport
  implements Serializable
{
  private static final long serialVersionUID = 3272473027285335936L;
  private static Logger logger = Logger.getLogger(DBSupport.class);
  private static String connectionXml;
  private static String queryXml;
  
  
  @SuppressWarnings("static-access")
  protected DBSupport(String connectionXml, String queryXml) {
	this.setConnectionXml(connectionXml);
	this.setQueryXml(queryXml);
  }
  
  protected DBSupport() {

  }
 
 protected static String getdataFromListConstraint(List<String> listConstraint){
	 
	 String data = "";
     for(int j=0;j<listConstraint.size();j++){
    	 
   	  if(j!=listConstraint.size()-1){
   		
   		  data+="'"+listConstraint.get(j)+"',";
   	  }else{
   		  data+="'"+listConstraint.get(j)+"'";
   	  }
     }
	 
	 return data;
 }
  
@SuppressWarnings({ "finally", "unchecked", "rawtypes" })
protected static List getDatas(String id, Map constraint)
  {
    List<Map<String, String>> datas = new ArrayList();
    Map<String, String> mapConstraint = new HashMap();
    if (constraint != null) {
      mapConstraint = constraint;
    }
    Connection connection = getConnection(getConnectionXml());
    try
    {
     
      Statement stmt = connection.createStatement();
      String query = XMLReader.getQuery(id,getQueryXml());
      logger.info("Query : ");
      logger.info(query);
      if (mapConstraint.keySet() != null)
      {
        Set<String> keysString = mapConstraint.keySet();
        String[] keys = (String[])keysString.toArray(new String[keysString.size()]);
        for (int i = 0; i < keys.length; i++)
        {
          String key = keys[i];
          if(query.contains(":"+key)&&query.contains("#"+key)){
        	  throw new Exception("Error caused same id in key "+key);
          }
          
          query = query.replaceAll(":" + key, "'" + (String)mapConstraint.get(key) + "'").replaceAll("#" + key, (String)mapConstraint.get(key));
        }
      }
      logger.info("Executing query...");
      ResultSet rs = stmt.executeQuery(query);
      
      ResultSetMetaData rsmd = rs.getMetaData();
      
      int columnsNumber = rsmd.getColumnCount();
      while (rs.next())
      {
        Map<String, String> data = new HashMap();
        for (int i = 1; i <= columnsNumber; i++) {
          data.put(rsmd.getColumnLabel(i), rs.getString(i));
        }
        datas.add(data);
      }
      connection.close();
    }
    catch (Exception e)
    {
      logger.info(e.toString());
      e.printStackTrace();
      connection.close();
    }
    finally
    {
      return datas;
    }
  }

@SuppressWarnings({ "finally", "unchecked", "rawtypes" })
protected static List getDatasByLimit(String id, Map constraint, int startPosition, int limit)
  {
    List<Map<String, String>> datas = new ArrayList();
    Map<String, String> mapConstraint = new HashMap();
    if (constraint != null) {
      mapConstraint = constraint;
    }
    Connection connection = getConnection(getConnectionXml());
    try
    {
     
      Statement stmt = connection.createStatement();
      String query = XMLReader.getQuery(id,getQueryXml());
      query = query + " limit "+Integer.toString(startPosition)+","+Integer.toString(limit);
      logger.info("Query : ");
      logger.info(query);
      if (mapConstraint.keySet() != null)
      {
        Set<String> keysString = mapConstraint.keySet();
        String[] keys = (String[])keysString.toArray(new String[keysString.size()]);
        for (int i = 0; i < keys.length; i++)
        {
          String key = keys[i];
          if(query.contains(":"+key)&&query.contains("#"+key)){
        	  throw new Exception("Error caused same id in key "+key);
          }
          
          query = query.replaceAll(":" + key, "'" + (String)mapConstraint.get(key) + "'").replaceAll("#" + key, (String)mapConstraint.get(key));
        }
      }
      logger.info("Executing query...");
      ResultSet rs = stmt.executeQuery(query);
      
      ResultSetMetaData rsmd = rs.getMetaData();
      
      int columnsNumber = rsmd.getColumnCount();
      while (rs.next())
      {
        Map<String, String> data = new HashMap();
        for (int i = 1; i <= columnsNumber; i++) {
          data.put(rsmd.getColumnLabel(i), rs.getString(i));
        }
        datas.add(data);
      }
      connection.close();
    }
    catch (Exception e)
    {
      logger.info(e.toString());
      e.printStackTrace();
      connection.close();
    }
    finally
    {
      return datas;
    }
  }


  @SuppressWarnings("unchecked")
private static Connection getConnection(String connectionXml)
  {
    Map<String, String> connectionMap = XMLReader.getDBConfigXML(connectionXml);
    Connection connection = null;
    try
    {
      logger.info("Get Connection...");
      Class.forName((String)connectionMap.get("driver_class"));
      connection = DriverManager.getConnection((String)connectionMap.get("connection_url"), (String)connectionMap.get("username"), 
        (String)connectionMap.get("password"));
      logger.info("Connection Success...");
    }
    catch (Exception e)
    {
      e.printStackTrace();
      logger.error("Connection error...");
      logger.error(e.toString());
    }
    return connection;
  }


protected static String getConnectionXml() {
	return connectionXml;
}


protected static void setConnectionXml(String connectionXml) {
	DBSupport.connectionXml = connectionXml;
}


protected static String getQueryXml() {
	return queryXml;
}


protected static void setQueryXml(String queryXml) {
	DBSupport.queryXml = queryXml;
}

}
