package com.okta.bulkload;

import java.io.*;
import java.util.*;
import java.io.FileReader;

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

    public static void main(String args[]) throws Exception{
        System.out.println("Start : "+new Date());
        System.out.println();
        
        if (args.length == 0)
        {
            System.out.println(new Date() + " : **ERROR** : Missing configuration file argument");
            System.out.println("Run using following command : ");
            System.out.println("java -jar bulk_load.jar <config_file>");
            System.exit(-1);
        }
        try{
            configuration.load(new FileInputStream(args[0]));
        }
        catch(Exception e){
            //TODO: Log
        }
        String org = configuration.getProperty("org");
        String apiToken = configuration.getProperty("apiToken");
        String csvFile = configuration.getProperty("csvFile");
        String errorFile = configuration.getProperty("errorFile");
        String csvHeaderRow = configuration.getProperty("csvHeaderRow");
        String[] csvHeaders = csvHeaderRow.split(",");
        String csvLoginField = configuration.getProperty("csvLoginField");
        
        int successCount = 0, errorCount = 0;
        CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
		
		//initialize the CSVParser object
		CSVParser parser = new CSVParser(new FileReader(csvFile), format);
        CSVPrinter errorRecordPrinter = new CSVPrinter(new FileWriter(errorFile),format);
        
        for(CSVRecord record : parser){
            JSONObject user = new JSONObject();
            JSONObject creds = new JSONObject();
            JSONObject profile = new JSONObject();
            
            //Add username
            profile.put("login", record.get(csvLoginField));
            //Flesh out rest of profile
            for (String headerColumn:csvHeaders)
                profile.put(configuration.getProperty("csvHeader."+headerColumn),record.get(headerColumn));
            
            creds.put("password", new JSONObject("{\"value\": \""+RandomStringUtils.randomAlphabetic(8)+"\"}"));
            
            user.put("profile", profile);
            user.put("credentials", creds);
            // Submit to Okta via CSV import endpoint
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
                Map values = record.toMap();
                values.put("errorCode", errorCode);
                values.put("errorCause", errorCause);
                if (errorCount==0)
                    errorRecordPrinter.printRecord(values.keySet());
                errorRecordPrinter.printRecord(values.values());//Got an error for this row - write it to error file
                System.err.flush();
                errorCount++;
            }
            else
                successCount++;
            if (successCount!=0 && successCount%10==0)System.out.print(".");
		}
		//close the parser and errorPrinter
		parser.close();
        errorRecordPrinter.close();

        System.out.println();
        System.out.println("Successfully added "+successCount+" user(s)");
        System.out.println("Error in processing "+errorCount+" user(s)"+(errorCount>0?". Failed rows in Errors.csv":""));
        System.out.println();
        System.out.println("Done : "+new Date());        
    }
}