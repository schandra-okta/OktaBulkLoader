package com.okta.bulkload;

import static com.okta.bulkload.BulkLoader.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
/**
 *
 * @author schandra
 */

public class BulkLoader {
    final static Properties configuration = new Properties();
    protected static volatile int successCount = 0, errorCount = 0;
    protected static CSVPrinter errorRecordPrinter;
    protected static volatile boolean noMoreRecordsBeingAdded = false;
    protected static volatile boolean errorHeaderWritten = false;

    public static void main(String args[]) throws Exception{
        System.out.println("Start : "+new Date());
        System.out.println();
        long startTime = System.currentTimeMillis();
        
        if (args.length == 0)
        {
            System.out.println(new Date() + " : **ERROR** : Missing configuration file argument");
            System.out.println("Run using following command : ");
            System.out.println("java -jar okta-bulkload.jar <config_file>");
            System.exit(-1);
        }
        try{
            configuration.load(new FileInputStream(args[0]));
        }
        catch(Exception e){
            //TODO: Log
        }
        String errorFile = configuration.getProperty("errorFile");
        int numConsumers = Integer.parseInt(configuration.getProperty("numConsumers", "1"));
        int bufferSize = Integer.parseInt(configuration.getProperty("bufferSize", "10000"));
        
        CSVFormat errorFormat = CSVFormat.RFC4180.withDelimiter(',');		
        errorRecordPrinter = new CSVPrinter(new FileWriter(errorFile),errorFormat);
        
        BlockingQueue myQueue = new LinkedBlockingQueue(bufferSize);
        
        Producer csvReadWorker = new Producer(myQueue);
        Thread producer = new Thread(csvReadWorker);
        producer.start();
        Thread.sleep(500);//Give the queue time to fill up
        
        Thread[] consumers = new Thread[numConsumers];
        for (int i = 0; i < numConsumers; i++){
            Consumer worker = new Consumer(myQueue);
            consumers[i] = new Thread(worker);
            consumers[i].start();
        }
        
        producer.join();
        for (int i = 0; i < numConsumers; i++)
            consumers[i].join();

		//close the errorPrinter
        errorRecordPrinter.close();
        
        System.out.println();
        System.out.println("Successfully added "+successCount+" user(s)");
        System.out.println("Error in processing "+errorCount+" user(s)"+(errorCount>0?". Failed rows in error file configured.":""));
        System.out.println();
        System.out.println("Done : "+new Date());
        long endTime = System.currentTimeMillis();
        long duration = (endTime - startTime)/1000;
        System.out.println("Total time taken = "+duration+" seconds");
    }
}

class Producer implements Runnable {
    private final BlockingQueue queue;
    private final String csvFile;
    private final CSVFormat format;
    Producer(BlockingQueue q) { 
        queue = q; 
        csvFile = configuration.getProperty("csvFile");
        format = CSVFormat.RFC4180.withHeader().withDelimiter(',');        
    }
    public void run() {
        try {
            //initialize the CSVParser object
            CSVParser parser = new CSVParser(new FileReader(csvFile), format);
            for(CSVRecord record : parser)           
                queue.add(record);
            parser.close();
        } catch (Exception excp) { 
            System.out.println(excp.getLocalizedMessage());
        } finally {
            noMoreRecordsBeingAdded = true;
        }
    }
}
   
 class Consumer implements Runnable {
    private final BlockingQueue queue;
    private final String org;
    private final String apiToken;
    private final String csvHeaderRow;
    private final String[] csvHeaders;
    private final String csvLoginField;
    Consumer(BlockingQueue q) { 
        queue = q; 
        org = configuration.getProperty("org");
        apiToken = configuration.getProperty("apiToken");
        csvHeaderRow = configuration.getProperty("csvHeaderRow");
        csvHeaders = csvHeaderRow.split(",");
        csvLoginField = configuration.getProperty("csvLoginField");
    }
    public void run() {
      try {
        while (true) { 
            if (noMoreRecordsBeingAdded && queue.isEmpty())
                Thread.currentThread().interrupt();
            consume(queue.take());
        }
      } catch (InterruptedException ex) { 
          System.out.println("Finished processing for this thread");
      } catch (Exception excp) { 
          System.out.println(excp.getLocalizedMessage());
      }     
    }
   
    void consume(Object record) throws Exception{
        CSVRecord csvRecord = (CSVRecord)record;
        JSONObject user = new JSONObject();
        JSONObject creds = new JSONObject();
        JSONObject profile = new JSONObject();

        //Add username
        profile.put("login", csvRecord.get(csvLoginField));
        //Flesh out rest of profile
        for (String headerColumn:csvHeaders)
            profile.put(configuration.getProperty("csvHeader."+headerColumn),csvRecord.get(headerColumn));

        creds.put("password", new JSONObject("{\"value\": \""+RandomStringUtils.randomAlphabetic(8)+"\"}"));

        user.put("profile", profile);
        user.put("credentials", creds);
        CloseableHttpClient httpclient = HttpClients.createDefault();

        // Build JSON payload
        StringEntity data = new StringEntity(user.toString(),ContentType.APPLICATION_JSON);

        // build http request and assign payload data
        HttpUriRequest request = RequestBuilder
                .post("https://"+org+"/api/v1/users")
                .setHeader("Authorization", "SSWS " + apiToken)
                .setEntity(data)
                .build();
        HttpResponse httpResponse = httpclient.execute(request);

        if (httpResponse.getStatusLine().getStatusCode() != 200){
            JSONObject errorJSON = new JSONObject(EntityUtils.toString(httpResponse.getEntity()));
            String errorCode = errorJSON.getString("errorCode");
            String errorCause = errorJSON.getJSONArray("errorCauses").getJSONObject(0).getString("errorSummary");
            Map values = csvRecord.toMap();
            values.put("errorCode", errorCode);
            values.put("errorCause", errorCause);
            if (!errorHeaderWritten){
                synchronized(errorRecordPrinter){
                    if(!errorHeaderWritten){
                        errorRecordPrinter.printRecord(values.keySet());
                        errorHeaderWritten = true;
                    }
                }
            }
            synchronized(errorRecordPrinter){
                errorRecordPrinter.printRecord(values.values());//Got an error for this row - write it to error file
            }
            errorCount++;
        }
        else
            successCount++;
        if (successCount!=0 && successCount%10==0)System.out.print(".");
    }
}