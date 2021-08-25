package com.kafka.thierno.twitter;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    static Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    public static final String TWITTER_CONSUMER_KEY = System.getenv("TWITTER_CONSUMER_KEY");
    public static final String TWITTER_CONSUMER_SECRET = System.getenv("TWITTER_CONSUMER_SECRET");
    public static final String TWITTER_TOKEN = System.getenv("TWITTER_TOKEN");
    public static final String TWITTER_SECRET = System.getenv("TWITTER_SECRET");

    /**
     * Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
     */
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
    //BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        //twitter client
        Client client = createTwitterClient();
        // Attempts to establish a connection.
        client.connect();

        //kafka producer

        //send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }

            if (msg != null) {
                logger.info("message: {}", msg);
            }
        }
        logger.info("End of the application");
    }

    public Client createTwitterClient() {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        // List<Long> followings = Lists.newArrayList(1234L, 566788L);
        // hosebirdEndpoint.followings(followings);
        List<String> terms = Lists.newArrayList("java", "microservices", "expressentry", "bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET, TWITTER_TOKEN, TWITTER_SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                //.eventMessageQueue(eventQueue)                          // optional: use this if you want to process client events
                ;

        return builder.build();
    }
}
