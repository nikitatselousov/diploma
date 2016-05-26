package com.nik.diploma;

import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class CalculateDistance extends EvalFunc<Tuple> {
    private static final Logger LOG = Logger.getLogger(CalculateDistance.class.getName());
    private enum InputTuple {
        TUPLES_BAG,
    };
    private enum BagTuple {
        DEVICE_ID,
        TIMESTAMP,
        LEVEL,
        PLUG_STATE,
        APPLICATION,
        FLAG,
    }

    private enum OutputTuple {
        Accident_Index, Latitude, Longitude, ident, latitude_deg, longitude_deg, iso_country, dist;
        public static String[] names() {
            OutputTuple[] states = values();
            String[] names = new String[states.length];

            for (int i = 0; i < states.length; i++) {
                names[i] = states[i].name();
            }

            return names;
        }
    };

    private static Map<String, List<Float>> airportsCoords = new HashMap<>();

    private static final String RESULT_BAG = "ResultBag";
    private static final String RESULT_TUPLE = "ResultTuple";
    static PrintWriter wr = null;
    static{
        try {
            File file = new File ("/usr/local/diploma/out.txt");
            wr = new PrintWriter(file);
        }
        catch (Exception e){}

    }

    @Override
    public Tuple exec(Tuple input) throws IOException {
        Object i = input.get(0);
        Tuple output = null;
        if (i == null){
            ArrayList<Float> coords = new ArrayList<>(2);
            coords.add((Float)input.get(4));
            coords.add((Float)input.get(5));
            airportsCoords.put((String)input.get(3),coords);
            return null;
        }
        else if (input.get(1) != null) {
            Double min = new Double(Double.MAX_VALUE);
            String min_ident = new String();
            for (Map.Entry<String, List<Float>> airport : airportsCoords.entrySet()){
                Double dist = distFrom((double)(airport.getValue().get(0)), (double)airport.getValue().get(1),(Float)input.get(1),(Float)input.get(2));
                if (min > dist) {
                    min = dist;
                    min_ident = airport.getKey();
                }
            }
            input.set(3, min_ident);
            input.append(min);
        }
        return input;
    }

    public static Double distFrom(double lat1, double lng1, double lat2, double lng2) {
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

    @Override
    public Schema outputSchema(Schema schema) {
        try {
            Schema tupleSchema = tupleSchema(schema);

            Schema.FieldSchema tupleFS = new Schema.FieldSchema(RESULT_TUPLE, tupleSchema, DataType.TUPLE);

            Schema bagSchema = new Schema(tupleFS);
            Schema.FieldSchema bagFS = new Schema.FieldSchema(RESULT_BAG, bagSchema, DataType.BAG);

            return new Schema(bagFS);

        } catch (Exception e) {
            //     logger.error(String.format("Error while creating output schema: %s", e.getMessage()));
            return null;
        }
    }

    public Schema tupleSchema(Schema schema) throws Exception {
        Schema tupleSchema = new Schema();
        String [] kekus = OutputTuple.names();
        for (int i = 0; i < kekus.length; i++){
            if (kekus[i].equalsIgnoreCase("dist") || kekus[i].equalsIgnoreCase("Longitude") || kekus[i].equalsIgnoreCase("Latitude"))
                tupleSchema.add(new Schema.FieldSchema(kekus[i].toString(), DataType.FLOAT));
            else
                tupleSchema.add(new Schema.FieldSchema(kekus[i].toString(), DataType.CHARARRAY));
        }
        return tupleSchema;
    }
}
