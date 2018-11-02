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

            final List<SensorMessage> allSensorMessages = new ArrayList<SensorMessage>();
            final List<String> allEventIDs = new ArrayList<String>();
            final List<LocalTime> allTimeStamps = new ArrayList<LocalTime>();

            long startTime = System.currentTimeMillis();
            LocalTime startTime1 = LocalTime.now();
            LocalTime startTimePlus1Min = startTime1.plusMinutes(1);
            while (LocalTime.now().isBefore(startTimePlus1Min)) {

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

                    //Check if messages are duplicates before adding them to allEventIDs and timestamp to allTimeStamps
                    if(doesMessageEventIdAlreadyExistInAllEventIDsList(allEventIDs, sensorMessage)) {
                        allEventIDs.add(sensorMessage.eventId);
                        allTimeStamps.add(sensorMessage.timeFormattedTimeStamp);
                        allSensorMessages.add(sensorMessage);
                    }

                    //prints out each message body as its received
                    System.out.println(sensorMessage);

                    long endTime = System.currentTimeMillis();
//                    startTime = removeOldMessages(allSensorMessages, startTime, endTime);
                }

            }
            List<String> allLocationIDs = getListOfLocationID(allSensorMessages);
            getListOfSensorReadingsByLocationID(allLocationIDs, allSensorMessages);

        } catch (AmazonServiceException e) {
            defaultErrorMessage(e.getErrorMessage());
        } catch (IOException e) {
            defaultErrorMessage(e.getMessage());
        }
    }

    private static long removeOldMessages(List<SensorMessage> allSensorMessages, long startTime, long endTime) {
        ArrayList<SensorMessage> remove = new ArrayList<SensorMessage>();
        if(endTime - startTime > 10 * 1000) {
            startTime = endTime;
            LocalTime tenSecondsAgo = LocalTime.now().minusSeconds(10);
            for(SensorMessage message : allSensorMessages) {
                if(message.timeFormattedTimeStamp.isBefore(tenSecondsAgo)) {
                    remove.add(message);

                }
            }
            for(SensorMessage i: remove){
                allSensorMessages.remove(i);

            }
            System.out.println("Received Sensor readings" + allSensorMessages);
        }
        return startTime;
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

    private static boolean doesMessageEventIdAlreadyExistInAllEventIDsList(List<String> allReadings, SensorMessage sensorMessage) {
       return !allReadings.contains(sensorMessage.eventId);
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

    public static List<String> getListOfLocationID(List<SensorMessage> allSensorReadings) {

        List<String> allLocationIDs = new ArrayList<String>();

        for(SensorMessage sensorReading : allSensorReadings) {
            if(!allLocationIDs.contains(sensorReading.locationId))
            allLocationIDs.add(sensorReading.locationId);
        }
        //Have a list of all the location IDs present
        return allLocationIDs;
    }

    public static void getListOfSensorReadingsByLocationID(List<String> allLocationIDs, List<SensorMessage> allSensorReadings) {

        for(String locationID: allLocationIDs) {
            double totalValue = 0.0;
            List<SensorMessage> allSensorReadingsByLocationID = new ArrayList<SensorMessage>();
            for(SensorMessage sensorReading: allSensorReadings) {
                if (locationID.equals(sensorReading.locationId)) {
                    allSensorReadingsByLocationID.add(sensorReading);
                }
            }
            for(SensorMessage sensorMessage: allSensorReadingsByLocationID) {
                totalValue =+ sensorMessage.value;
            }
//            System.out.println(allSensorReadingsByLocationID);
            System.out.println("location ID: " + locationID + " Value: " + totalValue);
        }

    }




    /*1. go through each sensor reading and make a list of all location ids
      2. go through each location id in that list and add the sensor readings that match that location ID
      3. go through each sensor reading for each location id and add values together
    */
}

