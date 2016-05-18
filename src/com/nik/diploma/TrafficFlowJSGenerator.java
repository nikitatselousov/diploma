package com.nik.diploma;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

/**
 * Created by nik on 02.03.16.
 */
public class TrafficFlowJSGenerator {

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
        File collData = new File("/media/762486602486236B__/diploma/link-coordinates_latlng.csv");

        CSVParser collParser = CSVParser.parse(collData, Charset.forName("UTF-8"), CSVFormat.RFC4180);
        PrintWriter writer = new PrintWriter("/home/nik/traffic_count_links.js", "UTF-8");
        writer.println("var routes = [");


        for (CSVRecord csvRecord : collParser){

            writer.println(",{coords :[");
            writer.println("["+csvRecord.get(9)+","+csvRecord.get(10)+"],");
            writer.println("["+csvRecord.get(11)+","+csvRecord.get(12)+"]],");
            writer.println("routeid:'"+csvRecord.get(0)+"'}");
        }
        writer.println("]};");
        writer.close();

        File collData1 = new File("/home/nik/DfTRoadSafety_Accidents_2014.csv");

        CSVParser collParser1 = CSVParser.parse(collData1, Charset.forName("UTF-8"), CSVFormat.RFC4180);
        JSONObject links = new JSONObject();
        links.put("type", "FeatureCollection");
        JSONArray features = new JSONArray();
        int i=0;

        for (CSVRecord csvRecord : collParser1){
            JSONObject arr = new JSONObject();
            arr.put("type", "Feature");
            arr.put("id", i++);
            JSONObject geometry = new JSONObject();
            geometry.put("type", "Point");
            LinkedList<Double> k = new LinkedList<>();
            k.add(Double.parseDouble(csvRecord.get(4)));
            k.add(Double.parseDouble(csvRecord.get(3)));
            geometry.put("coordinates", new JSONArray(k));
            arr.put("geometry", geometry);
            //JSONObject properties = new JSONObject();
            //properties.put("balloonContent", "Содержимое балуна");
            //arr.put("properties", properties);

            //if ((csvRecord.get(9)).matches("../01/....") || (csvRecord.get(9)).matches("../02/....") || (csvRecord.get(9)).matches("../03/....")|| (csvRecord.get(9)).matches("../04/....") || (csvRecord.get(9)).matches("../05/....") || (csvRecord.get(9)).matches("../06/....") || (csvRecord.get(9)).matches("../07/...."))
            if ((csvRecord.get(14).equalsIgnoreCase("1")) || (csvRecord.get(14).equalsIgnoreCase("2")) || (csvRecord.get(14).equalsIgnoreCase("3")))
                features.put(arr);
        }
        links.put("features", features);
        PrintWriter writer1 = new PrintWriter("/usr/local/diploma/html/data1.json", "UTF-8");
        writer1.print(links.toString());
        writer1.close();
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
