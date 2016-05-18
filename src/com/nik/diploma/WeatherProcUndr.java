package com.nik.diploma;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by nik on 02.03.16.
 */
public class WeatherProcUndr {

    public static class Station{
        String src_id;
        String lon;
        String lat;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Station station = (Station) o;

            return src_id.equals(station.src_id);

        }

        @Override
        public int hashCode() {
            return src_id.hashCode();
        }
    }

    public static void mane() throws Exception{
        Set<Station> st = new HashSet<Station>();
        File collData = new File("/usr/local/diploma/collisions.csv");
        File stationsData = new File("/usr/local/diploma/airports.csv");

        CSVParser collParser = CSVParser.parse(collData, Charset.forName("UTF-8"), CSVFormat.RFC4180);
        CSVParser stationsParser = CSVParser.parse(stationsData, Charset.forName("UTF-8"), CSVFormat.RFC4180);
        PrintWriter writer = new PrintWriter("fullundr.csv", "UTF-8");

        for (CSVRecord csvRecord : stationsParser) {
            Station station = new Station();
            station.src_id = (csvRecord.get(1));
            station.lat = (csvRecord.get(4));
            station.lon = (csvRecord.get(5));

            if (!st.contains(station)) {
                st.add(station);
            }
        }

        boolean firstTime = true;
        for (CSVRecord csvRecord : collParser){
            if (firstTime) {
                firstTime = false;
                continue;
            }
            Double min = new Double(100000000);
            String min_id = new String();
            Double collLong = Double.parseDouble(csvRecord.get(3));
            Double collLat = Double.parseDouble(csvRecord.get(4));

            for (Station s : st) {
                Double dist = distFrom(collLat, collLong, Double.parseDouble(s.lat), Double.parseDouble(s.lon));
                if (min > dist) {
                    min = dist;
                    min_id = s.src_id;
                }
            }

            String newLine = new String();
            for (int i=0; i< csvRecord.size();i++) {
                if (newLine != "")
                    newLine = newLine + ",";
                newLine = newLine + csvRecord.get(i);
            }
            newLine = newLine + "," + min_id + ","+ min.toString();
            writer.println(newLine);
        }
        writer.close();
    }

    public static Double distFrom(Double lat1, Double lng1, Double lat2, Double lng2) {
        double earthRadius = 6371000; //meters
        double dLat = Math.toRadians(lat2-lat1);
        double dLng = Math.toRadians(lng2-lng1);
        double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                        Math.sin(dLng/2) * Math.sin(dLng/2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
        Double dist = (Double) (earthRadius * c);

        return dist;
    }
}
