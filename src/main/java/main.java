import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import org.joda.time.LocalTime;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class main {

    public static void main(String[] args) {

        String bucket_name = "apprentices2018gasmon-locationss3bucket-7cafghqlcfch";
        String key_name = "locations.json";

        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        try {
            S3Object o = s3.getObject(bucket_name, key_name);
            S3ObjectInputStream s3is = o.getObjectContent();
            String result = getStringFromInputStream(s3is);
            s3is.close();

            GsonBuilder gsonBuilder = new GsonBuilder();
            Gson gson = gsonBuilder.create();

            Location[] locations = gson.fromJson(result, Location[].class);
            List<Location> locationInfo = new ArrayList<Location>(Arrays.asList(locations));

            System.out.println(locationInfo);
            System.out.println(result);

        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
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
