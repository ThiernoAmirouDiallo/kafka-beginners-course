package com.kafka.thierno.elasticsearch;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import com.google.gson.JsonParser;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerElasticSearch {

	static Logger logger = LoggerFactory.getLogger( KafkaConsumerElasticSearch.class );

	public static RestHighLevelClient restHighLevelClient() {
		String hostname = "kafka-poc-8680331189.us-east-1.bonsaisearch.net";
		String bonsaiAccessKey = System.getenv( "BONSAI_ACCESS_KEY" );
		String bonsaiAccessSecret = System.getenv( "BONSAI_ACCESS_SECRET" );

		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials( AuthScope.ANY, new UsernamePasswordCredentials( bonsaiAccessKey, bonsaiAccessSecret ) );

		RestClientBuilder builder = RestClient.builder( new HttpHost( hostname, 443, "https" ) ) //
				.setHttpClientConfigCallback( httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider( credentialsProvider ) );

		return new RestHighLevelClient( builder );
	}

	public static KafkaConsumer<String, String> createConsumer(String topic) {

		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "kafka-demo-elasticsearch";

		//properties
		Properties properties = new Properties();
		properties.setProperty( BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
		properties.setProperty( KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName() );
		properties.setProperty( VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName() );
		properties.setProperty( GROUP_ID_CONFIG, groupId );
		properties.setProperty( AUTO_OFFSET_RESET_CONFIG, "earliest" );
		properties.setProperty( ENABLE_AUTO_COMMIT_CONFIG, "false" ); //disable auto commit of offsets
		properties.setProperty( MAX_POLL_RECORDS_CONFIG, "20" );

		//consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>( properties );

		//subscribe consumer to topics
		consumer.subscribe( Collections.singletonList( topic ) );

		return consumer;
	}

	public static void main( String[] args ) throws InterruptedException {

		//sample request
		pushElasticSearch( UUID.randomUUID().toString(), "{ \"foo\": \"bar\"}", XContentType.JSON);

		KafkaConsumer<String, String> consumer = createConsumer( "twitter-tweets" );

		//poll new data
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll( Duration.ofMillis( 200 ) );

			logger.info( "Received {} kafka records", records.count() );

			BulkRequest bulkRequest = new BulkRequest();

			for ( ConsumerRecord<String, String> record : records ) {
				logger.info( "Pushing to elasticsearch:\n\tKey --> {} \n\tValue: {}\ttopic: {}\toffset: {}\n\tpartition: {}\n\ttimestamp: {}", record.key(), record.value(), record.topic(),
						record.offset(), record.partition(), record.timestamp() );

				/*
				 2 strategies for id,
				 kafka specific id
				 String id = record.topic() + record.partition() + record.offset()
				 source id -> twitter feed id
				 */
				String id = extractTweetId( record.value() );

				//pushElasticSearch( id, record.value(), XContentType.JSON ); // to push one record at a time to elastic search (not very performant, see bulkRequest below)
				bulkRequest.add( //
						new IndexRequest( "twitter" ) //
								.type( "tweets" ) //
								.id( id ) // to make to kafka consumer idenpotent because by default the the develivery-semantic is at-least-one meaning the same kafka record can be read twice
								.source( record.value(), XContentType.JSON ) );
			}

			if ( records.count() > 0 ) {
				bulkPushElasticSearch( bulkRequest ); //send multiple request to elasticsearch at once

				logger.info( "Commiting offsets" );
				consumer.commitSync();
				logger.info( "Offsets have been committed" );

				Thread.sleep( 1000 ); // adding a small delay
			}
		}
	}

	private static String extractTweetId( String jsonTweet ) {
		return JsonParser.parseString( jsonTweet ).getAsJsonObject().get( "id_str" ).getAsString();
	}

	private static void pushElasticSearch(String id, String source, XContentType contentType) {
		IndexRequest indexRequest = new IndexRequest( "twitter" ) //
				.type( "tweets" ) //
				.id( id ) // to make to kafka consumer idenpotent because by default the the develivery-semantic is at-least-one meaning the same kafka record can be read twice
				.source( source, contentType );

		try ( RestHighLevelClient client = restHighLevelClient() ) {
			IndexResponse indexResponse = client.index( indexRequest, RequestOptions.DEFAULT );
			logger.info( "Response id: {}, response status: {}\n\n", indexResponse.getId(), indexResponse.status().name() );
		} catch ( IOException e ) {
			logger.error( "Error while creating elasticsearch index", e );
		}
	}

	private static void bulkPushElasticSearch( BulkRequest bulkRequest ) {
		try ( RestHighLevelClient client = restHighLevelClient() ) {
			BulkResponse bulkResponse = client.bulk( bulkRequest, RequestOptions.DEFAULT );
			logger.info( "BulkResponse itemCount: {}, response status: {}\n\n", Arrays.stream( bulkResponse.getItems() ).count(), bulkResponse.status().name() );
		} catch ( IOException e ) {
			logger.error( "Error while sending bulkrequest to elasticsearch", e );
		}
	}

}
