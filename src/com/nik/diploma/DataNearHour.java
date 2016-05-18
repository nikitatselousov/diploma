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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

public class DataNearHour extends EvalFunc<Tuple> {
    private static final Logger LOG = Logger.getLogger(DataNearHour.class.getName());
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
        Tate,Accident_Index,Longitude,Latitude,Time,Weather_Conditions,Road_Surface_Conditions,w_src,dist,DATE, TIME,SRC_ID, PRST_WX_ID1,PRST_WX_ID2;
        public static String[] names() {
            OutputTuple[] states = values();
            String[] names = new String[states.length];

            for (int i = 0; i < states.length; i++) {
                names[i] = states[i].name();
            }

            return names;
        }
    };

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
        Tuple kek = null;
        Integer cnt = null;
            if (i instanceof  DataBag){
                Iterator iter = ((DataBag)i).iterator();
                if (iter.hasNext())
                    output = (Tuple) iter.next();
                else return input;
                //LOG.info(output.toDelimitedString(",") + cnt);
                DateFormat formatter = new SimpleDateFormat("HH:mm");
                Date accTime;
                Date tupleTime;
                try {
                    accTime = (Date) formatter.parse((String) output.get(4));
                    Date min = new Date(140000000);
                    while (iter.hasNext()){
                        kek = (Tuple)iter.next();
                        //LOG.info(kek.toDelimitedString(",") + (++cnt));
                        tupleTime = (Date) formatter.parse((String) kek.get(10));
                        if (accTime.getHours() == tupleTime.getHours() - 1){
                            output = kek;
                        }
                        if (accTime.getHours() == tupleTime.getHours() + 1){
                            cnt = (Integer)kek.get(12);
                        }
                    }
                }
                catch(Exception e){
                }
            }
        output.append(cnt);
        return output;
    }


    @Override
    public Schema outputSchema(Schema schema) {
        try {
            Schema tupleSchema = tupleSchema(schema);

            Schema.FieldSchema tupleFS = new Schema.FieldSchema(RESULT_TUPLE, tupleSchema, DataType.TUPLE);

            Schema bagSchema = new Schema(tupleFS);
            Schema.FieldSchema bagFS = new Schema.FieldSchema(RESULT_BAG, bagSchema, DataType.BAG);

            return new Schema(tupleFS);

        } catch (Exception e) {
            //     logger.error(String.format("Error while creating output schema: %s", e.getMessage()));
            return null;
        }
    }

    public Schema tupleSchema(Schema schema) throws Exception {
        Schema tupleSchema = new Schema();
        String [] kekus = OutputTuple.names();
        for (int i = 0; i < kekus.length; i++){
            if (kekus[i].equalsIgnoreCase("dist") || kekus[i].equalsIgnoreCase("Latitude") || kekus[i].equalsIgnoreCase("Longtitude"))
                tupleSchema.add(new Schema.FieldSchema(kekus[i].toString(), DataType.DOUBLE));
            else if (kekus[i].equalsIgnoreCase("SRC_ID")|| kekus[i].equalsIgnoreCase("PRST_WX_ID1")|| kekus[i].equalsIgnoreCase("PRST_WX_ID2")|| kekus[i].equalsIgnoreCase("Weather_Conditions") || kekus[i].equalsIgnoreCase("Road_Surface_Conditions"))
                tupleSchema.add(new Schema.FieldSchema(kekus[i].toString(), DataType.INTEGER));
            else
                tupleSchema.add(new Schema.FieldSchema(kekus[i].toString(), DataType.CHARARRAY));
        }
        return tupleSchema;
    }
}
