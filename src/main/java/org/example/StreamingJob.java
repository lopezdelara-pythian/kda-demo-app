package org.example;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.example.datatypes.*;
import org.example.operators.AggregateSales;
import org.example.operators.CollectSales;
import org.example.operators.EnrichSales;
import org.example.utils.ParameterToolUtils;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class StreamingJob {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);

        ParameterTool parameters;

        if (env instanceof LocalStreamEnvironment) {
            // read the parameters specified from the command line
            parameters = ParameterTool.fromArgs(args);
        } else {
            // read the parameters from the Kinesis Analytics environment
            Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
            Properties demoAppConfig = applicationProperties.get("DemoAppConfig");
            if (demoAppConfig == null) {
                throw new RuntimeException("Unable to load DemoAppConfig properties from the Kinesis Analytics " +
                        "Runtime.");
            }
            parameters = ParameterToolUtils.fromApplicationProperties(demoAppConfig);
        }

        // configuration for the consumers
        Properties consumerConfig = new Properties();
        consumerConfig.setProperty("bootstrap.servers", parameters.get("BootstrapServers"));
        consumerConfig.setProperty("group.id", parameters.get("GroupId"));
        consumerConfig.setProperty("auto.offset.reset", "earliest");

        // create the bookstores stream
        DataStream<Bookstore> bookstores = env.addSource(
                new FlinkKafkaConsumer<Bookstore>(
                        parameters.get("BookstoresTopic"),
                        new BookstoreSchema(),
                        consumerConfig)
        ).name("bookstores");

        // create the purchases stream
        DataStream<Purchase> purchases = env.addSource(
                new FlinkKafkaConsumer<Purchase>(
                        parameters.get("PurchasesTopic"),
                        new PurchaseSchema(),
                        consumerConfig)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Purchase>forMonotonousTimestamps()
                        .withIdleness(Duration.ofSeconds(10))
                        .withTimestampAssigner((purchase, timestamp) -> purchase.timestamp.getTime())
        ).name("purchases");

        // calculate the number of books sold and the sales amount by bookstore for each window
        DataStream<Sales> sales = purchases.keyBy(purchase -> purchase.bookstoreId)
                .window(TumblingEventTimeWindows.of(Time.of(60, TimeUnit.SECONDS)))
                .aggregate(new AggregateSales(), new CollectSales());

        // enrich the aggregated totals with the bookstore info
        DataStream<EnrichedSales> enrichedSales = sales.keyBy(s -> s.bookstoreId)
                .connect(bookstores.keyBy(b -> b.bookstoreId))
                .process(new EnrichSales());

        // create a sink for the enriched bookstore sales
        enrichedSales.addSink(
                new FlinkKafkaProducer<EnrichedSales>(
                        parameters.get("BootstrapServers"),
                        parameters.get("SalesTopic"),
                        new EnrichedSalesSchema())
        ).name("sales");

        // execute program
        env.execute("kda-demo-app");
    }
}
