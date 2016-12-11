package com.ibm.injest;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.messagehub.samples.MessageHubJavaSample;
import com.messagehub.samples.env.MessageList;

public class WorkerThread implements Runnable {
  
    private String topic;
    private String ftpPath;
    
    public WorkerThread(String topic,String ftpPath){
        this.topic=topic;
        this.ftpPath=ftpPath;
    }

    @Override
    public void run() {
		   try{
			    System.out.println("Worked Thread Started");
			    //MessageHubJavaSample proxy=new MessageHubJavaSample(topic);

		   }catch(Exception t){t.printStackTrace();}
		    finally{System.out.println("Ingested all the data*****************");}
	  }
    }