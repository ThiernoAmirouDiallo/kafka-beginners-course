package com.kafka.thierno.elasticsearch;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
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

	public static void main( String[] args ) {



		IndexRequest indexRequest = new IndexRequest( "twitter" ).type( "tweets" ).source( "{ \"foo\": \"bar\"}", XContentType.JSON );

		try ( RestHighLevelClient client = restHighLevelClient() ) {
			IndexResponse indexResponse = client.index( indexRequest, RequestOptions.DEFAULT );
			logger.info( "Response id: {}, response status: {}", indexResponse.getId(), indexResponse.status().name() );
		} catch ( IOException e ) {
			logger.error( "Error while creating elasticsearch index", e );
		}
	}

}
