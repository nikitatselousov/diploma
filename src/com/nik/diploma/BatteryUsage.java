package com.nik.diploma;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.log4j.Logger;
import org.apache.commons.collections.Bag;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class BatteryUsage extends EvalFunc<Tuple> {
    private static final Logger LOG = Logger.getLogger(BatteryUsage.class.getName());
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
        DATE, TIME,ID,ID_TYPE,MET_DOMAIN_NAME,SRC_ID ,REC_ST_IND,WIND_SPEED_UNIT_ID ,SRC_OPR_TYPE ,WIND_DIRECTION ,WIND_SPEED ,PRST_WX_ID ,PAST_WX_ID_1 ,PAST_WX_ID_2 ,CLD_TTL_AMT_ID ,LOW_CLD_TYPE_ID,MED_CLD_TYPE_ID,HI_CLD_TYPE_ID ,CLD_BASE_AMT_ID,CLD_BASE_HT,VISIBILITY ,MSL_PRESSURE ,VERT_VSBY,AIR_TEMPERATURE ,DEWPOINT,WETB_TEMP,STN_PRES ,ALT_PRES ,GROUND_STATE_ID ,Q10MNT_MXGST_SPD ,CAVOK_FLAG ,CS_HR_SUN_DUR,WMO_HR_SUN_DUR, SNOW_DEPTH ,DRV_HR_SUN_DUR;        public static String[] names() {
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
        //DataBAg putBa
            if (i instanceof  DataBag){
                Iterator iter = ((DataBag)i).iterator();
                if (iter.hasNext())
                    output = (Tuple) iter.next();
                else return input;

                while (iter.hasNext()){
                    Tuple tup = (Tuple)iter.next();
                    Double sum [] = new Double[output.size()];
                    int count [] = new int[output.size()];
                    for (int j = 0; j < sum.length; j++){
                        sum[j]=0.0;
                        count[j]=0;
                    }

                    for (int j=6; j<output.size(); j++){
                        String outStr = (String)output.get(j);
                        String tupStr = (String)tup.get(j);
                        if (tupStr.equalsIgnoreCase(outStr) || tupStr == null || tupStr.equalsIgnoreCase("") || tupStr.equalsIgnoreCase(" "))
                            continue;
                        if (outStr == null || outStr.equalsIgnoreCase(" ") || outStr.equalsIgnoreCase("")){
                            output.set(j, tupStr);
                        }
                        else {
                            //try {
                                //sum[j] = sum[j] + Double.parseDouble(tupStr.trim());
                                //count[j]++;
                                //output.set(j, ((Double) (sum[j] / count[j])).toString());
                            //}
                            //catch (Exception e){
                            //    LOG.info(tupStr);
                            //}
                        }
                    }
                }
            }

        return output;
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
            if (kekus[i].equalsIgnoreCase("SRC_ID"))
                tupleSchema.add(new Schema.FieldSchema(kekus[i].toString(), DataType.INTEGER));
            else
                tupleSchema.add(new Schema.FieldSchema(kekus[i].toString(), DataType.CHARARRAY));
        }
        return tupleSchema;
    }
}
