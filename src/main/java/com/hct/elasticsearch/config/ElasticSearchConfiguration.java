package com.hct.elasticsearch.config;

import com.hct.elasticsearch.config.properties.ElasticSearchProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class ElasticSearchConfiguration {

    private ElasticSearchProperties elasticSearchProperties;

    public ElasticSearchConfiguration(ElasticSearchProperties elasticSearchProperties) {
        this.elasticSearchProperties = elasticSearchProperties;
    }

    @Bean(destroyMethod = "close")
    public RestHighLevelClient elasticSearchClient() throws Exception {

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(elasticSearchProperties.getUsername(), elasticSearchProperties.getPassword()));

        //ECDHE_RSA
        Registry<SchemeIOSessionStrategy> sessionStrategyRegistry = RegistryBuilder.<SchemeIOSessionStrategy>create()
                .register( "http", NoopIOSessionStrategy.INSTANCE )
                .register( "https", new SSLIOSessionStrategy(
                        SSLContexts.custom().loadTrustMaterial(null, new TrustSelfSignedStrategy()).build(),
                        new NoopHostnameVerifier()) )
                .build();

        RestClientBuilder builder = RestClient
                .builder(new HttpHost(elasticSearchProperties.getHost(), elasticSearchProperties.getPort(), elasticSearchProperties.getScheme()))
                .setHttpClientConfigCallback(
                        httpClientBuilder -> httpClientBuilder
                                .setDefaultCredentialsProvider(credentialsProvider)
                                .setSSLStrategy(sessionStrategyRegistry.lookup(elasticSearchProperties.getScheme().toLowerCase()))
                );

        return new RestHighLevelClient(builder);

    }

}