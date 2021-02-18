package demo;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ElasticSearchConsumer {
    public static RestHighLevelClient createClient() {
        String hostname = "localhost";

        // chain on setDefaultCredentialsProvider() for supply cred to remote node if necessary
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 9200, "http"));

        return new RestHighLevelClient(builder);
    }

    public static KafkaConsumer<String, String> createConsumer(List<String> topics) {
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "kafka-elasticsearch-demo";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe consumer to topic(s)
        consumer.subscribe(topics);

        return consumer;
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        // create elasticsearch client
        RestHighLevelClient client = createClient();

        // create kafka consumer
        KafkaConsumer<String, String> consumer = createConsumer(Arrays.asList("twitter_tweets"));

        // poll for new data from broker
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record: records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());

                // kafka generic id
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                // twitter feed id
                String id = getIdFromTweet(record.value());


                // send data to elasticsearch
                // make sure index exists otherwise a new index will be created
                // Note: Offsets are committed at least once (not at most) by default,
                //      need to make sure duplicates are not being created by replicas.
                //      Index with id to make request idempotent, use either kafka or twitter id
                IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id)
                        .source(record.value(), XContentType.JSON);

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

                logger.info(indexResponse.getId());

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // client.close();

    }

    private static String getIdFromTweet(String tweetJson) {
        return JsonParser
                .parseString(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }
}
