package training.gasmon;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.util.Topics;
import com.amazonaws.services.sqs.model.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import org.joda.time.LocalTime;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class main {

    public static void main(String[] args) {

//        try {
//            TimeUnit.SECONDS.sleep(60);
//        } catch (InterruptedException e){
//            defaultErrorMessage(e.getMessage());
//        }

        String bucket_name = "apprentices2018gasmon-locationss3bucket-7cafghqlcfch";
        String key_name = "locations.json";
        String topicARN = "arn:aws:sns:eu-west-1:552908040772:Apprentices2018GasMon-snsTopicSensorDataPart1-1TGJVE8L26XKE";
        String queueUrl = null;

        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        try {
            S3Object o = s3.getObject(bucket_name, key_name);
            S3ObjectInputStream s3is = o.getObjectContent();
            String result = getStringFromInputStream(s3is);
            s3is.close();

            List<Location> locationInfo = getListOfLocationsFromJson(result);
            System.out.println(locationInfo);

            AmazonSQS queueClient = AmazonSQSClientBuilder.defaultClient();

            ListQueuesResult lq_result = queueClient.listQueues();
            System.out.println("Your SQS Queue URLs:");
            for (String url : lq_result.getQueueUrls()) {
                System.out.println(url);
                if(url.contains("Jess")){
                    deleteQueue(url);
                }
            }

            queueUrl = createQueueRequest(queueUrl, queueClient);

            AmazonSNS topicClient = AmazonSNSClientBuilder.defaultClient();

            Topics.subscribeQueue(topicClient, queueClient, topicARN, queueUrl);

            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e){
                defaultErrorMessage(e.getMessage());
            }

            System.out.println("about to create messages");
            List<Message> messages = queueClient.receiveMessage(queueUrl).getMessages();
            for(Message ms : messages ) {
                System.out.println(ms);
            }


        } catch (AmazonServiceException e) {
            defaultErrorMessage(e.getErrorMessage());
        } catch (IOException e) {
            defaultErrorMessage(e.getMessage());
        }
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

    private static void deleteQueue(String queueUrl) {
        AmazonSQS sqsDelete = AmazonSQSClientBuilder.defaultClient();
        sqsDelete.deleteQueue(queueUrl);
    }

    private static void defaultErrorMessage(String errorMessage) {
        System.err.println(errorMessage);
        System.exit(1);
    }

    private static List<Location> getListOfLocationsFromJson(String result) {
        GsonBuilder gsonBuilder = new GsonBuilder();
        Gson gson = gsonBuilder.create();

        Location[] locations = gson.fromJson(result, Location[].class);
        return new ArrayList<Location>(Arrays.asList(locations));
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
}
