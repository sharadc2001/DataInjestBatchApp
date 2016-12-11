package com.ibm.injest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
//import java.nio.charset.Charset;
//import java.nio.file.Files;
//import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Level;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.messagehub.samples.MessageHubJavaSample;
import com.messagehub.samples.env.MessageHubCredentials;
import com.messagehub.samples.env.MessageHubEnvironment;
import com.messagehub.samples.env.MessageList;

@ApplicationPath("rest")
@Path("sharadtestsample")
public class TestService  extends Application{
	  private String topic="SampleTopic";
	  private String ftpPath="";
	  private String line="";
	 private  ExecutorService executor = Executors.newFixedThreadPool(5);

	@GET
	@Path("getNumArray")
	@Produces("application/json")
	public List<String> getSampleData(@QueryParam("value") String value) {
		return Arrays.asList(new String[] { "one", "two", "three", "four", value });
	}

	@GET
	@Path("getString")
	@Produces("application/json")
	public String getString() {
		System.out.println("Test called");
		return "Test";
	}

	  @GET
	  @Path("fetchData")
	  @Produces("application/json")
	  public String fetchData() {
		  System.out.println("fetch data called");
		   try{
			    MessageHubJavaSample proxy=new MessageHubJavaSample(topic);
	        	ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	        	InputStream fileStream = classLoader.getResourceAsStream("data/all.cars");
	        	InputStreamReader r = new InputStreamReader(fileStream);
	        	BufferedReader br = new BufferedReader(r);
	            MessageList list = new MessageList();
	            
	        	while((line=br.readLine())!=null){
	        		 list.push(line);
	           		 //proxy.InjestData(line);
	           		 //System.out.println("Injesting Data:: " +line);
	        	}			    
       		    proxy.InjestData(list);
       		 //System.out.println("Injesting Data:: " +line);
		   }catch(Exception t){t.printStackTrace();}
		    finally{System.out.println("Ingested all the data*****************");}
         
		   return "Success";

	  } 
	  
	  @GET
	  @Path("fetchDCData")
	  @Produces("application/json")
	  public String fetchDCData(@QueryParam("region")String region) {
		  System.out.println("fetch data called");
		     String temp=region.trim();
			 System.out.println("temp:: " +temp + " equalIgnoreCase:: " +temp.equals("USA"));
			  if(region.equalsIgnoreCase("ATL")){
				  topic="atl";
				  ftpPath="/data/atl/all.cars";
				  System.out.println("topic:: " +topic +"ftp:: " +ftpPath);
				  pushData(topic,ftpPath);
				  //pushDataThreaded(topic,ftpPath);
			   }else if(region.equalsIgnoreCase("USA")){
					  topic="usa";
					  ftpPath="/data/usa/FL_insurance_sample.csv";	
					  System.out.println("topic:: " +topic +"ftp:: " +ftpPath);
					  pushData(topic,ftpPath);
					  //pushDataThreaded(topic,ftpPath);
			   }else if(region.equalsIgnoreCase("IND")){
					  topic="ind";
					  ftpPath="/data/ind/Sacramentorealestatetransactions.csv";	
					  System.out.println("topic:: " +topic +"ftp:: " +ftpPath);
					  pushData(topic,ftpPath);
					  //pushDataThreaded(topic,ftpPath);
			   }else if(region.equalsIgnoreCase("PAK")){
					  topic="pak";
					  ftpPath="/data/pak/SalesJan2009.csv";	
					  System.out.println("topic:: " +topic +"ftp:: " +ftpPath);
					  pushData(topic,ftpPath);
					  //pushDataThreaded(topic,ftpPath);
			   }
   
		   return "Success";

	  } 

	  @GET
	  @Path("fetchAssetData")
	  @Produces("application/json")
	  public void fetchAssetData(@QueryParam("cat")String category) {
		  System.out.println("fetch data called");
		     String temp=category.trim();
			 System.out.println("temp:: " +temp + " equalIgnoreCase:: " +temp.equals("inv"));
	       	         
			  if(category.equalsIgnoreCase("inv")){
				  topic="inventory";
				  ftpPath="/data/inv/Sample_500000.csv";
				  System.out.println("topic:: " +topic +"ftp:: " +ftpPath);
				  //pushData(topic,ftpPath);
				  pushBatchData(topic,ftpPath);
			   }else if(category.equalsIgnoreCase("fac")){
					  topic="facility";
					  ftpPath="/data/fac/FL_insurance_sample.csv";	
					  System.out.println("topic:: " +topic +"ftp:: " +ftpPath);
					  pushData(topic,ftpPath);
					  //pushBatchData(topic,ftpPath);
			   }else if(category.equalsIgnoreCase("opor")){
					  topic="openorder";
					  ftpPath="/data/opor/Sacramentorealestatetransactions.csv";	
					  System.out.println("topic:: " +topic +"ftp:: " +ftpPath);
					  pushData(topic,ftpPath);
					  //pushBatchData(topic,ftpPath);
			   }else if(category.equalsIgnoreCase("shp")){
					  topic="shipment";
					  ftpPath="/data/shp/SalesJan2009.csv";	
					  System.out.println("topic:: " +topic +"ftp:: " +ftpPath);
					  pushData(topic,ftpPath);
					  //pushBatchData(topic,ftpPath);
			   }
   
		   //return "Success";

	  } 
	  
	  public void pushData(String topic,String datapath){
		   try{
			    MessageHubJavaSample proxy=new MessageHubJavaSample(topic);
	        	ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	        	InputStream fileStream = classLoader.getResourceAsStream(ftpPath);
	        	InputStreamReader r = new InputStreamReader(fileStream);
	        	BufferedReader br = new BufferedReader(r);
	        	System.out.println("Inside Push Data:: " +fileStream);
	        	while((line=br.readLine())!=null){
	        		System.out.println("Injesting Data:: " +line);
	        		proxy.InjestData(line);  
	        	}	
			   //System.out.println("Outside While");
		   }catch(Exception t){t.printStackTrace();}
		    finally{System.out.println("Ingested all the data*****************");}
	  }

	  public void pushBatchData(String topic,String datapath){
		  try{
			    MessageHubJavaSample proxy=new MessageHubJavaSample(topic);
			    int percentile=0;
			    int linecount=getLineCount(ftpPath);
			    if(linecount<10){percentile=1;}
			    else {percentile=40;}
			    int count=0;
			    int r=linecount%percentile;
			    int q=linecount/percentile;

			    System.out.println("Total Lines:: " +linecount + "Quotient:: " +q + " Reminder:: " +r);			    
	        	ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	        	InputStream fileStream = classLoader.getResourceAsStream(ftpPath);
	        	InputStreamReader reader = new InputStreamReader(fileStream);
	        	BufferedReader br = new BufferedReader(reader);
	        	System.out.println("Inside Push Data:: " +fileStream);
	        	
	        	MessageList list = new MessageList();
	        	 
	        	while((line=br.readLine())!=null){
	        		 //System.out.println("Injesting Data:: " +line);
	        		  //proxy.InjestData(line);  
	        		list.push(line);
	        		count++;
	        		if(count==q){
	        			//System.out.println("Injesting Data:: " +list.toString());
	        			proxy.InjestData(list);
	        		    count=0;
	        		    list=new MessageList();
	        		}
	        	}	
	        	
	        	if(r!=0){
	        		//System.out.println("Injesting Reminder Data:: " +list.toString());
	        		proxy.InjestData(list);
	        	}
		  }catch(Exception t){
			  t.printStackTrace();
		  }
	  }
	  
	  private int getLineCount(String ftpPath){
		  int count=0;
		  try{
	        	ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
	        	InputStream fileStream = classLoader.getResourceAsStream(ftpPath);
	        	InputStreamReader r = new InputStreamReader(fileStream);
	        	BufferedReader br = new BufferedReader(r);
	        	System.out.println("Inside Push Data:: " +fileStream);
	        	 MessageList list = new MessageList();
	        	while((line=br.readLine())!=null){
                       ++count;
	        	}				  
		  }catch(Exception t){t.printStackTrace();}
		  return count;
	  }

/*	  public void pushDataThreaded(String topic,String datapath){
		   try{
	            Runnable worker = new WorkerThread(topic,datapath);
	            executor.execute(worker);
		   }catch(Exception t){t.printStackTrace();}
		    finally{executor.shutdown();while (!executor.isTerminated()){}}
	  }
*/	  
}
