package training.gasmon;

import org.joda.time.LocalTime;

public class SensorMessage {

    public String locationId;
    public String eventId;
    public double value;
    public long timestamp;
    public LocalTime timeFormattedTimeStamp;

    public String toString() {
        return "locationId: " + locationId + " eventId: " + eventId +  " value: " + value + " time: " + timeFormattedTimeStamp;
    }

    public String getLocationId() {
        return locationId;
    }

    public String getEventId() {
        return eventId;
    }

    public double getValue() {
        return value;
    }

    public LocalTime getTimeFormattedTimeStamp() {
        return timeFormattedTimeStamp;
    }


}
