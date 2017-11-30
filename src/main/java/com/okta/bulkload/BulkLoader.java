package com.okta.bulkload;

import static com.okta.bulkload.BulkLoader.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.csv.*;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
/**
 *
 * @author schandra
 */

public class BulkLoader {
    final static Properties configuration = new Properties();
    protected static AtomicInteger successCount = new AtomicInteger(0), errorCount = new AtomicInteger(0);
    protected static CSVPrinter errorRecordPrinter, rateLimitFailurePrinter;
    protected static volatile boolean noMoreRecordsBeingAdded = false;
    protected static volatile boolean errorHeaderWritten = false;
    protected static String[] errorHeaders = null;
    protected static String csvFileArg = null;

    public static void main(String args[]) throws Exception{
        System.out.println("Start : "+new Date());
        System.out.println();
        long startTime = System.currentTimeMillis();
        
        if (args.length < 2)
        {
            System.out.println(new Date() + " : **ERROR** : Missing configuration file argument");
            System.out.println("Run using following command : ");
            System.out.println("java -jar bulk_load.jar <config_file> <csv_file_location>");
            System.exit(-1);
        }
        try{
            configuration.load(new FileInputStream(args[0]));
            csvFileArg = args[1];
        }
        catch(Exception e){
            System.out.println("Error reading configuration. Exiting...");
            System.exit(-1);
        }
        String filePrefix = csvFileArg.substring(0,csvFileArg.lastIndexOf('.'));	
        String errorFile = filePrefix+"_reject.csv";
        String rateLimitFile = filePrefix+"_replay.csv";
        errorHeaders = (configuration.getProperty("csvHeaderRow")+",errorCode,errorCause").split(",");
        int numConsumers = Integer.parseInt(configuration.getProperty("numConsumers", "1"));
        int bufferSize = Integer.parseInt(configuration.getProperty("bufferSize", "10000"));
        
        CSVFormat errorFormat = CSVFormat.RFC4180.withDelimiter(',').withQuote('"').withQuoteMode(QuoteMode.ALL).withHeader(errorHeaders);        
        errorRecordPrinter = new CSVPrinter(new FileWriter(errorFile),errorFormat);
        rateLimitFailurePrinter = new CSVPrinter(new FileWriter(rateLimitFile),errorFormat);
        errorRecordPrinter.flush();
        rateLimitFailurePrinter.flush();
        
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
        System.out.println("Error in processing "+errorCount+" user(s)");
        System.out.println();
        System.out.println("Done : "+new Date());
        long endTime = System.currentTimeMillis();
        long duration = (endTime - startTime)/1000;
        System.out.println("Total time taken = "+duration+" seconds");
    }
}

class Producer implements Runnable {
    private final BlockingQueue queue;
    private final CSVFormat format;
    Producer(BlockingQueue q) { 
        queue = q; 
        format = CSVFormat.RFC4180.withHeader().withDelimiter(',');        
    }
    public void run() {
        try {
            //initialize the CSVParser object
            CSVParser parser = new CSVParser(new FileReader(csvFileArg), format);
            for(CSVRecord record : parser)           
                queue.put(record);
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
    private final CloseableHttpClient httpclient;
    Consumer(BlockingQueue q) { 
        queue = q; 
        org = configuration.getProperty("org");
        apiToken = configuration.getProperty("apiToken");
        csvHeaderRow = configuration.getProperty("csvHeaderRow");
        csvHeaders = csvHeaderRow.split(",");
        csvLoginField = configuration.getProperty("csvLoginField");
        httpclient = HttpClientBuilder.create().setRetryHandler(new DefaultHttpRequestRetryHandler(3, false)).build();
    }
    public void run() {
        try {
            while (true) { 
                if (noMoreRecordsBeingAdded && queue.isEmpty())
                    Thread.currentThread().interrupt();
                consume(queue.take());
            }
        } catch (InterruptedException ex) { 
            //System.out.println("Finished processing for this thread");
        } catch (Exception excp) { 
            System.out.println(excp.getLocalizedMessage());//This consumer thread will abort execution
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

        // Build JSON payload
        StringEntity data = new StringEntity(user.toString(),ContentType.APPLICATION_JSON);

        // build http request and assign payload data
        HttpUriRequest request = RequestBuilder
                .post("https://"+org+"/api/v1/users")
                .setHeader("Authorization", "SSWS " + apiToken)
                .setEntity(data)
                .build();
        CloseableHttpResponse httpResponse = null;
        try{
            httpResponse = httpclient.execute(request);
            int responseCode = httpResponse.getStatusLine().getStatusCode();
            
            //Rate limit exceeded, hold off processing for this thread till the limit is reset
            if (responseCode == 429){//Retry after appropriate time
                handleErrorResponse(true, responseCode, httpResponse, csvRecord, null);
                long limitResetsAt = Long.parseLong(httpResponse.getFirstHeader("x-rate-limit-reset").getValue());
                //Put this thread to sleep for at least 5 seconds
                long timeToSleep = Math.abs(limitResetsAt - (System.currentTimeMillis()/1000)) + 5;
                Thread.sleep(timeToSleep*1000);
            }
            else if (responseCode != 200){//Non-success
                handleErrorResponse(false, responseCode, httpResponse, csvRecord, "");
            }
            else
                successCount.getAndIncrement();
            if (successCount.get()!=0 && successCount.get()%100==0)System.out.print(".");
        } catch(Exception e){//Issue with the connection. Let's not lose the consumer thread
            handleErrorResponse(false, 400, httpResponse, csvRecord, e.getLocalizedMessage());
        }finally{
            if (null != httpResponse)
                httpResponse.close();
        }
    }
    
    void handleErrorResponse(boolean isRateLimitError, int responseCode, CloseableHttpResponse response, CSVRecord csvRecord, String exceptionMessage)throws IOException{
        String errorCode, errorCause;
        try{
            JSONObject errorJSON = new JSONObject(EntityUtils.toString(response.getEntity()));
            errorCode = errorJSON.getString("errorCode");
            errorCause = errorJSON.getJSONArray("errorCauses").getJSONObject(0).getString("errorSummary");
        }catch (Exception e){
            //Can't get error details out of JSON. Assume error that did not result from data
            errorCode = "HTTP Response code : "+responseCode;
            errorCause = exceptionMessage;
        }
        Map values = csvRecord.toMap();
        values.put("errorCode", errorCode);
        values.put("errorCause", errorCause);
        if(isRateLimitError)
        {
            synchronized(rateLimitFailurePrinter){
                for (String header : errorHeaders)
                    rateLimitFailurePrinter.print(values.get(header));//Got an error for this row - write it to error file
                rateLimitFailurePrinter.println();
                rateLimitFailurePrinter.flush();
            }
        }
        else{
            synchronized(errorRecordPrinter){
                for (String header : errorHeaders)
                    errorRecordPrinter.print(values.get(header));//Got an error for this row - write it to error file
                errorRecordPrinter.println();
                errorRecordPrinter.flush();
            }
        }
        errorCount.getAndIncrement();
    }
}
