package com.github.rayjc.kafka.demo;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    private final String consumerKey = System.getenv("TWITTER_API_KEY");
    private final String consumerSecretKey = System.getenv("TWITTER_SECRET_KEY");
    private final String token = System.getenv("TWITTER_TOKEN");
    private final String tokenSecret = System.getenv("TWITTER_TOKEN_SECRET");
    private final String bootstrapServers = "127.0.0.1:9092";

    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private List<String> terms;

    public TwitterProducer(List<String> terms) {
        this.terms = terms;
    }

    public static void main(String[] args) {
        new TwitterProducer(Lists.newArrayList("final fantasy", "kingdom hearts")).run();
    }

    public void run() {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // create twitter client
        Client client = createTwitterClient(msgQueue);

        // create kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application...");
            logger.info("Terminating Twitter client...");
            client.stop();
            logger.info("Terminating Kafka producer...");
            producer.close();
            logger.info("Finished...");
        }));

        // stream tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if (msg != null) {
                logger.info(msg);
                // remember to create a topic
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            logger.error("Got an error...", e);
                        }
                    }
                });
            }

        }

        logger.info("Application Exiting.");
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        return new KafkaProducer<String, String>(properties);
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecretKey, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        // try to establish a connection
        hosebirdClient.connect();

        return hosebirdClient;
    }
}
