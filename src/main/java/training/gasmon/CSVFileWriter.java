//package training.gasmon;
//
//import training.gasmon.SensorMessage;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//
//public class CSVFileWriter {
//    private static final String COMMA_DELIMITTER = ",";
//    private static final String NEW_LINE_SEPARATOR = "\n";
//    private static final String FILE_HEADER = "locationId,eventId,value,timeFormattedTimeStamp";
//
//    public static void writeCSVFile(String fileName, List<SensorMessage> allSensorMessages) {
//
//        FileWriter fileWriter = null;
//        try {
//            fileWriter = new FileWriter(fileName);
//            fileWriter.append(FILE_HEADER.toString());
//            fileWriter.append(NEW_LINE_SEPARATOR);
//
//            for(SensorMessage message : allSensorMessages) {
//                fileWriter.append();
//            }
//        } catch (Exception e) {
//
//        }
//    }
//
//}
