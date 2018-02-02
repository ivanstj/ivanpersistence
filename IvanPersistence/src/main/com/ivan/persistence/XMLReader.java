package com.ivan.persistence;

import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class XMLReader
  implements Serializable
{
  private static final long serialVersionUID = 2648884755543744627L;
  private static Logger logger = Logger.getLogger(XMLReader.class);
  
  @SuppressWarnings({ "finally", "rawtypes", "unchecked" })
public static Map getDBConfigXML(String xml)
  {
    Map<String, String> result = null;
    try
    {
      InputStream is = XMLReader.class.getResourceAsStream("/"+xml);
      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      Document doc = dBuilder.parse(is);
      doc.getDocumentElement().normalize();
      
      NodeList rootnList = doc.getElementsByTagName("configuration");
      if (rootnList.getLength() == 1)
      {
        Element elRoot = (Element)rootnList.item(0);
        NodeList nList = elRoot.getElementsByTagName("property");
        if (nList.getLength() > 0) {
          result = new HashMap();
        }
        for (int temp = 0; temp < nList.getLength(); temp++)
        {
          Element eElement = (Element)nList.item(temp);
          result.put(eElement.getAttribute("name"), eElement.getTextContent());
        }
      }
      else
      {
        if (rootnList.getLength() == 0) {
          throw new Exception("No configuration found!");
        }
        if (rootnList.getLength() > 1) {
          throw new Exception("Multiple configuration found!");
        }
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
      logger.error(e.toString());
    }
    finally
    {
    	return result;
    }
    
  }
  
  @SuppressWarnings("finally")
public static String getQuery(String id,String queryXml)
  {
    String result = null;
    try
    {
      InputStream is = XMLReader.class.getResourceAsStream("/"+queryXml);
      DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
      Document doc = dBuilder.parse(is);
      doc.getDocumentElement().normalize();
      
      NodeList rootnList = doc.getElementsByTagName("query");
      Element rootEl = (Element)rootnList.item(0);
      if (rootnList.getLength() == 1)
      {
        NodeList nList = rootEl.getElementsByTagName("sql-query");
        for (int temp = 0; temp < nList.getLength(); temp++)
        {
          Element eElement = (Element)nList.item(temp);
          if (eElement.getAttribute("id").equals(id))
          {
            result = eElement.getTextContent();
            break;
          }
        }
      }
      else
      {
        if (rootnList.getLength() == 0) {
          throw new Exception("No sql-query found!");
        }
        if (rootnList.getLength() > 1) {
          throw new Exception("Multiple sql-query found!");
        }
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
      logger.error(e.toString());
    }
    finally
    {
    	return result;
    }
  }
}
