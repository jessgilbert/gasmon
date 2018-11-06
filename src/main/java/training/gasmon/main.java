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
import org.joda.time.Period;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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

            LocalTime startTime = LocalTime.now();
            LocalTime startTimePlus6Mins = startTime.plusMinutes(6);
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

                    //Check if messages are duplicates before adding them to allEventIDs and timestamp to allTimeStamps
                    if(doesMessageEventIdAlreadyExistInAllEventIDsList(allEventIDs, sensorMessage)) {
                        allEventIDs.add(sensorMessage.eventId);
                        allTimeStamps.add(sensorMessage.timeFormattedTimeStamp);
                        allSensorMessages.add(sensorMessage);
                    }

                    //prints out each message body as its received
                    System.out.println(sensorMessage);

                    //if it has been 6 minutes since the start time
                    if(LocalTime.now().isAfter(startTime.plusMinutes(6))) {
                        //make a list of all the location ids received
                        List<String> allLocationIDs = getListOfLocationID(allSensorMessages);
                        String sb = getListOfSensorReadingsByLocationID(allLocationIDs, allSensorMessages, startTime);
                        try {
                            Files.write(Paths.get("test4.csv"), sb.getBytes(), StandardOpenOption.APPEND);
                        } catch(IOException e) {

                        }
                        System.out.println("done");
                        startTime = startTime.plusMinutes(1);
                    }

                }
            }

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

    public static String getListOfSensorReadingsByLocationID(List<String> allLocationIDs, List<SensorMessage> allSensorReadings, LocalTime startTime) throws FileNotFoundException{


        StringBuilder sb = new StringBuilder();
        sb.append('\n');

        //go through each location ID received
        for(String locationID: allLocationIDs) {
            sb.append(locationID);
            sb.append(',');

//            System.out.println(locationID);
            double totalValue = 0.0;
            int amountInList = 0;
            //make a list
            List<SensorMessage> allSensorReadingsByLocationID = new ArrayList<SensorMessage>();
            //for each sensor reading in the list of all sensor readings
            for(SensorMessage sensorReading: allSensorReadings) {
                if (locationID.equals(sensorReading.locationId) && sensorReading.timeFormattedTimeStamp.isAfter(startTime) && sensorReading.timeFormattedTimeStamp.isBefore(startTime.plusMinutes(1))) {
//                    System.out.println(sensorReading);
//                    System.out.println(sensorReading.value);
                    totalValue += sensorReading.value;
                    ++amountInList;
                    allSensorReadingsByLocationID.add(sensorReading);
                    // add the sensor reading to the list of sensor readings by location ID if the sensor readings location ID is equal to the current one in the list
                    // and if the time stamp is after the start time and before the start time after a minute
                }
            }
            //for each sensor message in the new list by location IDs add the value to totalValue and plus one to the amount in the list
//            System.out.println(allSensorReadingsByLocationID);
            // make total value equal to the values of all divided by the amount in this list
            totalValue = totalValue / amountInList;
//            System.out.println(amountInList);

            System.out.println("location ID: " + locationID + " Value: " + totalValue + " for time interval between " + startTime + " - " + startTime.plusMinutes(1));

            sb.append(totalValue);
            sb.append(',');
            sb.append(startTime.plusMinutes(1));
            sb.append(',');
        }
        return sb.toString();
    }

//    public static void CSVFileWriter(List<SensorMessage> ) throws FileNotFoundException {
//        PrintWriter printWriter = new PrintWriter(new File("test.csv"));
//        StringBuilder sb = new StringBuilder();
//        sb.append()
//    }


    /*1. go through each sensor reading and make a list of all location ids
      2. go through each location id in that list and add the sensor readings that match that location ID
      3. go through each sensor reading for each location id and add values together
      4. go through sensor readings with same location id and find timestamps that are a minute apart
      5. for each value in the sensor reading that have the same location ids and a minute interval apart plus the values together and divide by the amount of values added
      let it run for 6 minutes, then look from 5 minutes ago and a minute before (5min delay) to find min interval readings

      write locationID and value and start-end time to csv file
      import file to excel and get a graph for each locationID to show value for each time interval - see if increasing or decreasing
      OR
      split location ID to x and y  and plot values? not sure how it would work
    */
}

