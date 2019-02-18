package com.hct.elasticsearch.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hct.elasticsearch.dto.ElasticSearchQueryResponse;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class ElasticSearchApiService {

    private RestHighLevelClient elasticSearchClient;
    private ObjectMapper objectMapper;

    @Autowired
    ElasticSearchApiService(RestHighLevelClient elasticSearchClient, ObjectMapper objectMapper) {
        this.elasticSearchClient = elasticSearchClient;
        this.objectMapper = objectMapper;
    }

    public boolean hasIndex(String index) {
        try {
            GetIndexRequest request = new GetIndexRequest();
            request.indices(index);
            return elasticSearchClient.indices().exists(request, RequestOptions.DEFAULT);
        }
        catch (IOException ioe) {
            log.error("search errors:", ioe);
            throw new RuntimeException(ioe.getMessage());
        }
    }

    public List<ElasticSearchQueryResponse> querySearchAll(String index) {
        if (!hasIndex(index)) {
            return Collections.EMPTY_LIST;
        }
        try {
            SearchResponse searchResponse = elasticSearchClient.search(new SearchRequest(index), RequestOptions.DEFAULT);
            return convert(searchResponse.getHits());

        }
        catch (IOException ioe) {
            log.error("search errors:", ioe);
            throw new RuntimeException(ioe.getMessage());
        }
    }

    public ElasticSearchQueryResponse getDocumentById(String index, String type, String id) {

        GetRequest getRequest = new GetRequest();
        getRequest.index(index);
        getRequest.type(type);
        getRequest.id(id);
        try {
            GetResponse getResponse = elasticSearchClient.get(getRequest, RequestOptions.DEFAULT);
            if (getResponse == null || !getResponse.isExists()) {
                return null;
            }
            return ElasticSearchQueryResponse.builder()
                    .id(getResponse.getId())
                    .index(getResponse.getIndex())
                    .type(getResponse.getType())
                    .source(getResponse.getSourceAsString())
                    .build();
        }
        catch (ElasticsearchStatusException ese) {
            log.info("get document by index:" + index +",type:" + type + ",id:" + id, ese);
            return null;
        }
        catch (IOException ioe) {
            throw new RuntimeException("fetch query template errors", ioe);
        }
    }

    public void updateDocument(String index, String type, String id, String json) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest(index, type, id);
        updateRequest.doc(json, XContentType.JSON);
        UpdateResponse updateResponse = elasticSearchClient.update(updateRequest, RequestOptions.DEFAULT);
        if (updateResponse.status() != RestStatus.OK) {
            throw new RuntimeException("errors["+ updateResponse.status() +"] occur for updating index:" + index +", type:" + type +", id:" + id + ",json:" + json );
        }
    }

    public ElasticSearchQueryResponse saveDocument(String index, String type, String jsonSource) throws IOException {
        return saveDocument(index, type, null, jsonSource);
    }

    public ElasticSearchQueryResponse saveDocument(String index, String type, String id, String jsonSource) throws IOException {
        IndexRequest indexRequest = new IndexRequest(index, type, id);
        indexRequest.source(jsonSource, XContentType.JSON);
        ElasticSearchQueryResponse response = saveDocument(indexRequest);
        response.setSource(jsonSource);
        return response;
    }

    protected ElasticSearchQueryResponse saveDocument(IndexRequest indexRequest) throws IOException {

        String requestJson = indexRequest.toString();
        IndexResponse indexResponse = elasticSearchClient.index(indexRequest, RequestOptions.DEFAULT);

        if ((indexResponse.getResult() == DocWriteResponse.Result.CREATED &&
                indexResponse.status() == RestStatus.CREATED) ||
                (indexResponse.getResult() == DocWriteResponse.Result.UPDATED &&
                        indexResponse.status() == RestStatus.OK)
        ) {
            return ElasticSearchQueryResponse.builder()
                    .index(indexResponse.getIndex())
                    .type(indexResponse.getType())
                    .id(indexResponse.getId())
                    .build();
        }
        throw new RuntimeException("errors["+ indexResponse.status() +"] occur for saving index:" + requestJson);
    }

    public List<ElasticSearchQueryResponse> queryMatches(String index, String field, Object value) {
        SearchRequest searchRequest = buildMatchSearchRequest(index, field, value);
        try {
            SearchResponse searchResponse = elasticSearchClient.search(searchRequest, RequestOptions.DEFAULT);
            return convert(searchResponse.getHits());
        }
        catch (IOException ioe) {
            throw new RuntimeException("multi search errors", ioe);
        }
    }

    public List<ElasticSearchQueryResponse> queryMultiMatches(String index, Map<String, Object> arguments) {

        List<ElasticSearchQueryResponse> result = new ArrayList<>();
        try {
            MultiSearchRequest request = new MultiSearchRequest();

            for (String argKey : arguments.keySet()) {
                SearchRequest searchRequest = new SearchRequest(index);
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchSourceBuilder.query(QueryBuilders.matchQuery(argKey, arguments.get(argKey)));
                searchRequest.source(searchSourceBuilder);
                request.add(searchRequest);
            }

            MultiSearchResponse multiSearchResponse = elasticSearchClient.msearch(request, RequestOptions.DEFAULT);
            for (MultiSearchResponse.Item item : multiSearchResponse.getResponses()) {
                result.addAll(convert(item.getResponse().getHits()));
            }
        }
        catch (IOException ioe) {
            throw new RuntimeException("multi search errors", ioe);
        }
        return result;
    }

    public List<ElasticSearchQueryResponse> query(String index , String templateId, Map<String, Object> parameters) {

        try {
            SearchTemplateRequest request = new SearchTemplateRequest();
            request.setRequest(new SearchRequest(index));

            request.setScriptType(ScriptType.STORED);
            request.setScript(templateId);

            request.setScriptParams(parameters);

            SearchTemplateResponse response = elasticSearchClient.searchTemplate(request, RequestOptions.DEFAULT);
            return convert(response.getResponse().getHits());

        }
        catch (Exception ioe) {
            throw new RuntimeException("execute query errors", ioe);
        }

    }


    public void upsertTemplate(String templateName, String source) throws IOException {

        String endpoint = "_scripts/" + templateName;
        UpsertScriptRequest upsertScriptRequest = UpsertScriptRequest.builder()
                .script(ElasticSearchScript.builder()
                        .lang(MustacheScriptEngine.NAME)
                        .source(source)
                        .build())
                .build();
        String json = objectMapper.writeValueAsString(upsertScriptRequest);

        HttpEntity request = new StringEntity(json, ContentType.APPLICATION_JSON);

        Response response = elasticSearchClient.getLowLevelClient().performRequest(
                HttpPost.METHOD_NAME, endpoint, Collections.EMPTY_MAP, request);

        if (response.getStatusLine().getStatusCode() != RestStatus.OK.getStatus()) {
            throw new RuntimeException("errors["+ response.getStatusLine().getStatusCode() +"] occur for upserting template:" + templateName + ",json:" + json );
        }
    }

    public String getTemplateSource(String templateName) throws IOException {

        String endpoint = "_scripts/" + templateName;

        Response response = elasticSearchClient.getLowLevelClient().performRequest(
                HttpGet.METHOD_NAME, endpoint);

        if (response.getStatusLine().getStatusCode() != RestStatus.OK.getStatus()) {
            throw new RuntimeException("errors to get template source. template name=" + templateName );
        }

        StringWriter writer = new StringWriter();
        String encoding = StandardCharsets.UTF_8.name();
        IOUtils.copy(response.getEntity().getContent(), writer, encoding);

        String json = objectMapper.readTree(writer.toString()).path("script").path("source").asText();
        return json;
    }

    private SearchRequest buildMatchSearchRequest(String index, String field, Object value) {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(field, value));
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    private List<ElasticSearchQueryResponse> convert(Response response) throws IOException {

        StringWriter writer = new StringWriter();
        String encoding = StandardCharsets.UTF_8.name();
        IOUtils.copy(response.getEntity().getContent(), writer, encoding);

        JsonNode jsonNode = objectMapper.readTree(writer.toString()).get(SearchHits.Fields.HITS);
        long totalHits = jsonNode.get(SearchHits.Fields.TOTAL).asLong();

        List<ElasticSearchQueryResponse> result = new ArrayList<>();
        if (totalHits > 0) {
            for (JsonNode hitNode : jsonNode.get(SearchHits.Fields.HITS)) {

                ElasticSearchQueryResponse row = ElasticSearchQueryResponse.builder()
                        .id(hitNode.findValue("_id").asText())
                        .index(hitNode.findValue("_index").asText())
                        .type(hitNode.findValue("_type").asText())
                        .source(hitNode.findValue("_source").toString())
                        .build();
                result.add(row);
            }
        }
        return result;
    }

    private List<ElasticSearchQueryResponse> convert(SearchHits hits) {
        List<ElasticSearchQueryResponse> result = new ArrayList<>();
        if (hits == null || hits.totalHits == 0) {
            return result;
        }
        for(SearchHit hit: hits) {
            result.add(convert(hit));
        }
        return result;
    }

    private ElasticSearchQueryResponse convert(SearchHit hit) {
        return ElasticSearchQueryResponse.builder()
                .id(hit.getId())
                .type(hit.getType())
                .index(hit.getIndex())
                .source(hit.getSourceAsString())
                .build();
    }

    @Data
    @Builder
    private static class UpsertScriptRequest {
        private ElasticSearchScript script;
    }

    @Data
    @Builder
    private static class ElasticSearchScript {
        private String lang;
        private String source;
    }
}
