package com.kafka.thierno.kafkastream.filter.tweets;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;

import com.google.gson.JsonParser;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

public class StreamFilterTweets {

	public static void main( String[] args ) {

		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "demo-kafka-streams";

		//properties
		Properties properties = new Properties();
		properties.setProperty( BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
		properties.setProperty( APPLICATION_ID_CONFIG, groupId );
		properties.setProperty( DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName() );
		properties.setProperty( DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName() );

		// create topology
		StreamsBuilder streamsBuilder = new StreamsBuilder();

		// input topic
		KStream<String, String> inputKStream = streamsBuilder.stream( "twitter-tweets" );
		KStream<String, String> filteredKStream = inputKStream.filter( ( key, value ) -> extractNbFlowers( value ) > 10_000 );
		filteredKStream.to( "important-tweets" );

		// build the topology
		KafkaStreams kafkaStreams = new KafkaStreams( streamsBuilder.build(), properties );

		// start kafka streams application
		kafkaStreams.start();
	}

	private static long extractNbFlowers( String jsonTweet ) {
		try {
			return JsonParser.parseString( jsonTweet ) //
					.getAsJsonObject() //
					.get( "user" ) //
					.getAsJsonObject() //
					.get( "followers_count" ) //
					.getAsLong();
		} catch ( Exception e ) {
			return 0;
		}
	}

}
