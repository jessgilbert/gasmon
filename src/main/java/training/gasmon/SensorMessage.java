package training.gasmon;

import org.joda.time.LocalTime;

public class SensorMessage {

    public String locationId;
    public String eventId;
    public double value;
    public long timestamp;
    public LocalTime timeFormattedTimeStamp;

    public String toString() {
        return "locationID: " + locationId + " eventID: " + eventId +  " value: " + value + " time: " + timeFormattedTimeStamp;
    }

}
