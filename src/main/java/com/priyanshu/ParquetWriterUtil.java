package com.priyanshu;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.hadoop.ParquetWriter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class ParquetWriterUtil {
    public static void writeToParquet(String outputPath, Schema schema, List<GenericRecord> records) throws IOException {
        Path path = Path.of(outputPath);
        OutputFile outputFile = new LocalOutputFile(path);
        int DEFAULT_BLOCK_SIZE = 13417728;
        int DEFAULT_PAGE_SIZE = 1048576;
//        Schema schema = ReflectData.get().getSchema(Student.class);

        ParquetWriter<Object> writer = AvroParquetWriter.builder(outputFile)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(DEFAULT_BLOCK_SIZE)
                .withPageSize(DEFAULT_PAGE_SIZE)
                .build();
    }
}
