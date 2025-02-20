package com.priyanshu;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.OutputFile;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class ObjectStorageSink extends RichSinkFunction<Object>{

    private final String Url;
    private final String accessKey;
    private final String secretKey;
    private String bucketName;
    private String keyPrefix;
    private String filePath;
    private ParquetWriter writer;

    public ObjectStorageSink(String url, String accessKey, String secretKey, String bucketName, String keyPrefix, String filePath) {
        this.Url = url;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.bucketName = bucketName;
        this.keyPrefix = keyPrefix;
        this.filePath = filePath;
    }

    @Override
    public void open(Configuration config) throws Exception{
        super.open(config);
        Path path = Path.of(filePath);
        OutputFile outputFile = new LocalOutputFile(path);
        int DEFAULT_BLOCK_SIZE = 13417728;
        int DEFAULT_PAGE_SIZE = 1048576;
        Schema schema = ReflectData.get().getSchema(Student.class);

        OutputFileConfig fileConfig = OutputFileConfig
                .builder()
                .withPartPrefix("students") // Prefix for file names
                .withPartSuffix(".parquet") // Suffix for file names
                .build();

        writer = AvroParquetWriter.builder(outputFile)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(DEFAULT_BLOCK_SIZE)
                .withPageSize(DEFAULT_PAGE_SIZE)
                .build();
    }

    @Override
    public void invoke(Object obj, Context context) throws Exception{
        System.out.println("invoke of Sink called");
        System.out.println(obj.getClass());
        Schema schema = ReflectData.get().getSchema(Student.class);

        List<GenericRecord> records = new ArrayList<>();
        GenericRecord record = new GenericData.Record(schema);
        Student student = (Student) obj;
        record.put("id", student.getId());
        record.put("name", student.getName());
        record.put("marks", student.getMarks());
        records.add(record);

        writer.write(record);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (writer != null) {
            writer.close(); // Ensure the writer is closed and all data is flushed
        }
    }

}
