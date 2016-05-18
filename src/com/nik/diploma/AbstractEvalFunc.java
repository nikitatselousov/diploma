package com.nik.diploma;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public abstract class AbstractEvalFunc extends EvalFunc<DataBag> {

   // private static final Logger logger = Logger.getLogger(AbstractEvalFunc.class.getName());

    private static final String RESULT_BAG = "ResultBag";
    private static final String RESULT_TUPLE = "ResultTuple";

    public abstract DataBag eval(Tuple input) throws Exception;

    @Override
    public DataBag exec(Tuple input) throws IOException {
        try {
            return eval(input);
        } catch (Exception e) {
         //   logger.error(String.format("Error while evaluating input tuple: %s", e.getMessage()));
            throw new IOException(e);
        }
    }

    public abstract Schema tupleSchema(Schema schema) throws Exception;

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
}
