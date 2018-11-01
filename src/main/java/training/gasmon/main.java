package training.gasmon;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.xspec.S;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.util.Topics;
import com.amazonaws.services.sqs.model.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import org.joda.time.LocalTime;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class main {

    public static void main(String[] args) {

        String bucket_name = "apprentices2018gasmon-locationss3bucket-7cafghqlcfch";
        String key_name = "locations.json";
        String topicARN = "arn:aws:sns:eu-west-1:552908040772:Apprentices2018GasMon-snsTopicSensorDataPart1-1TGJVE8L26XKE";
        String queueUrl = null;


//                deleteOldMessages deleteOldMessage = new deleteOldMessages();
//        Thread thread = new Thread(deleteOldMessage);
//        thread.start();

        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

        try {
            AmazonSQS queueClient = AmazonSQSClientBuilder.defaultClient();

            S3Object o = s3.getObject(bucket_name, key_name);
            S3ObjectInputStream s3is = o.getObjectContent();
            String result = getStringFromInputStream(s3is);
            s3is.close();

            List<Location> locationInfo = getListOfLocations(result);
            System.out.println(locationInfo);

            ListQueuesResult lq_result = queueClient.listQueues();
            System.out.println("Your SQS Queue URLs:");
            for (String url : lq_result.getQueueUrls()) {
                System.out.println(url);
                if(url.contains("Jess")){
                    deleteQueue(url);
                }
            }

            //1. create queue request
            queueUrl = createQueueRequest(queueUrl, queueClient);

            AmazonSNS topicClient = AmazonSNSClientBuilder.defaultClient();

            Topics.subscribeQueue(topicClient, queueClient, topicARN, queueUrl);

            // messages request to receive messages
            ReceiveMessageRequest messageRequest = receiveMessageRequest(queueUrl);
            //wait 10 seconds so messages are received
            wait10Seconds();

            final List<String> allReadings = new ArrayList<String>();
            final List<LocalTime> allTimeStamps = new ArrayList<LocalTime>();

            long startTime = System.currentTimeMillis();
            while (true) {

                //List of Messages received
                List<Message> messages = queueClient.receiveMessage(messageRequest).getMessages();

                //for each message received
                for (Message ms : messages) {

                    GsonBuilder gsonBuilder = new GsonBuilder();
                    Gson gson = gsonBuilder.create();

                    //create an object SQSMessage of whole message and gets the body - sensor message
                    SQSMessage SQSMessage = gson.fromJson(ms.getBody(), SQSMessage.class);



                    //create sensor message object of each SQSMessage message body
                    final SensorMessage sensorMessage = gson.fromJson(SQSMessage.Message, SensorMessage.class);

                    //get timestamp from sensorMessage and convert to LocalTime
                    sensorMessage.timeFormattedTimeStamp = convertsSensorMessageTimeStampToLocalTime(sensorMessage);

                    //Check if messages are duplicates before adding them to allReadings and timestamp to allTimeStamps
                    checkIfMessageEventIdIsDuplicateBeforeAddingToAllReadings(allReadings, sensorMessage, allTimeStamps);

                    System.out.println(SQSMessage.Message);



                    long endTime = System.currentTimeMillis();
                    ArrayList<String> remove = new ArrayList<String>();
                    if(endTime - startTime > 10 * 1000) {
                        LocalTime tenSecondsAgo = LocalTime.now().minusSeconds(10);
                        for(String message : allReadings) {
                            if(sensorMessage.timeFormattedTimeStamp.isBefore(tenSecondsAgo)) {
                                remove.add(message);

                            }
                        }
                        for(String i: remove){
                            allReadings.remove(i);

                        }
                        System.out.println("new " + allReadings);
                    }


                }
            }

        } catch (AmazonServiceException e) {
            defaultErrorMessage(e.getErrorMessage());
        } catch (IOException e) {
            defaultErrorMessage(e.getMessage());
        }
    }

    private static ReceiveMessageRequest receiveMessageRequest(String queueUrl) {
        ReceiveMessageRequest request = new ReceiveMessageRequest(queueUrl).withMessageAttributeNames("ALL");
        request.setMaxNumberOfMessages(10);
        return request;
    }

    private static String getStringFromInputStream(S3ObjectInputStream s3is) {

        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();

        String line;
        try {

            br = new BufferedReader(new InputStreamReader(s3is));
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return sb.toString();

    }

    private static List<Location> getListOfLocationsFromJson(String result) {
        GsonBuilder gsonBuilder = new GsonBuilder();
        Gson gson = gsonBuilder.create();

        Location[] locations = gson.fromJson(result, Location[].class);
        return new ArrayList<Location>(Arrays.asList(locations));
    }

    private static List<Location> getListOfLocations(String result) {
        return getListOfLocationsFromJson(result);
    }

    private static String createQueueRequest(String queueUrl, AmazonSQS queueClient) {
        CreateQueueRequest create_request = new CreateQueueRequest("JessQ" + new Date().getTime())
                .addAttributesEntry("MessageRetentionPeriod", "86400");
        try {
            queueUrl = queueClient.createQueue(create_request).getQueueUrl();
        } catch (AmazonSQSException e) {
            if (!e.getErrorCode().equals("QueueAlreadyExists")) {
                throw e;
            }
        }
        return queueUrl;
    }

    private static void wait10Seconds() {
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e){
            defaultErrorMessage(e.getMessage());
        }
    }

    private static void checkIfMessageEventIdIsDuplicateBeforeAddingToAllReadings(List<String> allReadings, SensorMessage sensorMessage, List<LocalTime> allTimeStamps) {
        if(allReadings.contains(sensorMessage.eventId)) {
            System.out.println("Duplicate message! It has not been added");
        } else {
            allReadings.add(sensorMessage.eventId);
            allTimeStamps.add(sensorMessage.timeFormattedTimeStamp);
        }
    }

    private static void deleteQueue(String queueUrl) {
        AmazonSQS sqsDelete = AmazonSQSClientBuilder.defaultClient();
        sqsDelete.deleteQueue(queueUrl);
    }

    private static void defaultErrorMessage(String errorMessage) {
        System.err.println(errorMessage);
        System.exit(1);
    }

    private static LocalTime convertsSensorMessageTimeStampToLocalTime(SensorMessage sensorMessage) {

        Date date = new Date(sensorMessage.timestamp);
        DateFormat formatter = new SimpleDateFormat("HH:mm:ss.SSS");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        String stringFormattedTimeStamp = formatter.format(date);

        LocalTime timeFormattedTimeStamp = LocalTime.parse(stringFormattedTimeStamp);
        return timeFormattedTimeStamp;
    }


}

