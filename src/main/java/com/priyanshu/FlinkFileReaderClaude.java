package com.priyanshu;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.time.Duration;

public class FlinkFileReaderClaude {
    public static void main(String[] args) throws Exception {
        // Create Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);

        Schema schema = ReflectData.get().getSchema(Student.class);

        // Define input and output paths
        String inputPath = "file:///C:/Users/priya/Repository/flink-tutorial/random.dat";
        String outputPath = "file:///C:/Users/priya/Repository/flink-tutorial/parquet-output";

        // Create Hadoop Configuration
        Configuration hadoopConfig = new Configuration();
        hadoopConfig.set("fs.defaultFS", "file:///");  // For local filesystem

        // Read from DAT file and convert to DataStream
        DataStream<String> rawInput = env.readTextFile(inputPath)
                .setParallelism(1);  // Use single parallelism for file reading

        // Read from DAT file and convert to DataStream Method 2
        FileInputFormat<String> inputFormat = new TextInputFormat(new Path(inputPath));
        DataStreamSource<String> streamSource = env.readFile(inputFormat, inputPath);

        // Convert raw input to POJO
        DataStream<Student> records = rawInput.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                // Assuming DAT file has comma-separated values
                String[] fields = value.split("\\|");
                Student record = new Student();
                record.setId(fields[0]);
                record.setName(fields[1]);
                record.setMarks(fields[2]);
                return record;
            }
        });

        records.print();


        // Define the output file configuration
        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("students") // Prefix for file names
                .withPartSuffix(".parquet") // Suffix for file names
                .build();
        // Create Parquet sink
        final StreamingFileSink<Student> sink = StreamingFileSink
                .forBulkFormat(new Path(outputPath),
                        AvroParquetWriters.forReflectRecord(Student.class))
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH")) // Organize files into date-time buckets
                .withRollingPolicy(OnCheckpointRollingPolicy.build()) // Roll files on checkpoints
                .withOutputFileConfig(config) // Apply custom file naming
                .build();

        //writing generic writer
//        final ParquetWriter<GenericRecord> genericRecordParquetWriter = AvroParquetWriter.<GenericRecord>builder(
//                new org.apache.hadoop.fs.Path(outputPath)).withSchema(schema).withCompressionCodec(CompressionCodecName.SNAPPY).build();
//
//        final FileSink<String> sampleParquetSink = FileSink.forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
//                .withOutputFileConfig(config)
//                .withRollingPolicy(OnCheckpointRollingPolicy.build())
//                .build();

        //Using WriterFactory and FileSink
        ParquetWriterFactory<Student> studentParquetWriterFactory = ParquetAvroWriters.forReflectRecord(Student.class);
        FileSink<Student> studentParquetSink = FileSink.forBulkFormat(new Path(outputPath), studentParquetWriterFactory)
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

//        DefaultRollingPolicy.builder()
//                .withRolloverInterval(Duration.ofMinutes(5).toMillis()) // Rotate every 5 min
//                .withInactivityInterval(Duration.ofMinutes(2).toMillis()) // Close inactive files
//                .withMaxPartSize(128 * 1024 * 1024) // 128MB max part size
//                .build()


//        String url = "https://s3.amazonaws.com"; // S3 endpoint
//        String accessKey = "your-access-key";
//        String secretKey = "your-secret-key";
//        String bucketName = "your-bucket-name";
//        String keyPrefix = "your-folder-prefix";
//        ObjectStorageSink objectStorageSink = new ObjectStorageSink(url, accessKey, secretKey, bucketName, keyPrefix, outputPath+".parquet");

        records.sinkTo(studentParquetSink);

        // Write to Parquet
//        records.addSink(sink); //[DONT REMOVE]

        // Execute Flink job
        env.execute("DAT to Parquet Converter");
    }

}
