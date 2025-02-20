package com.priyanshu;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;

public class AvroSchemaSerializer extends Serializer<Schema> {
    @Override
    public void write(Kryo kryo, Output output, Schema schema) {
        output.writeString(schema.toString());
    }

    @Override
    public Schema read(Kryo kryo, Input input, Class<Schema> aClass) {
        String schemaString = input.readString();
        return new Schema.Parser().parse(schemaString);
    }
}
