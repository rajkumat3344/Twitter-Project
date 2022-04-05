package io.conduktor.Kafka;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class TwitterConsumer {
    public static RestHighLevelClient createClient() {
        String hostName = "kafka-javaes-9908720030.us-west-2.bonsaisearch.net";
        String userName = "r6mhpknwuz";
        String passWord = "rn30u7ykmr";

        /*Don't do if you run a local ES*/
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName,passWord));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostName,443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> createKafkaConsumer(String topic) {
        String host = "localhost:9094";
        String groupId = "kafka-demo-elasticsearch";

        /*KafkaConsumer properties*/
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");//poll offset increase/decrease as per requirement

        /*Create Consumer*/
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    private static JsonParser parser= new JsonParser();

    private static String extractIdFromTweet(String tweetJSON) {
        //gson library
        return JsonParser.parseString(tweetJSON)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(TwitterConsumer.class.getName());
        RestHighLevelClient elasticSearchClient = createClient();


        KafkaConsumer<String, String> consumer = createKafkaConsumer("twitter_tweets");

        try (elasticSearchClient; consumer) {

            /*Poll for new data - Consumer will not take the data until it asks for the data*/
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));//new in kafka 3.0.0
                Integer recordCount = records.count();
                logger.info("Received: " + recordCount + " records");

                BulkRequest bulkRequest = new BulkRequest();
                for (ConsumerRecord<String, String> record : records) {
                    /*Here we insert the data for ElasticSearch*/

                    /*2 strategies*/
                    /*Kafka generic id*/
                    //String id = record.topic()+ "_" + record.partition()+ "_" + record.offset();

                    try {
                        /*Twitter feed specific id*/
                        String id = extractIdFromTweet(record.value());
                        /*To inserting data*/
                        IndexRequest indexRequest = new IndexRequest(
                                "twitter",
                                "tweets",
                                id /*this is to make our consumer idempotent*/
                        ).source(record.value(), XContentType.JSON).id(id);


                        /*To use IndexRequest*/
                        bulkRequest.add(indexRequest);//we add to our bulk request(takes no time)
                    } catch (NullPointerException e) {
                        logger.warn("Skipping bad data: " + record.value());
                    }
                }
                if (recordCount > 0) {
                    BulkResponse bulkItemResponses = elasticSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    logger.info("Committing the offset...");
                    consumer.commitSync();
                    logger.info("Offset has been committed...");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

        } catch (Exception e) {

        }


        /*client.close();*/
    }
}

