package com.hct.elasticsearch.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hct.elasticsearch.config.properties.QueryConfigurationProperties;
import com.hct.elasticsearch.dto.ElasticSearchQueryResponse;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.index.IndexRequest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.Instant;
import java.util.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles("test")
public class ElasticSearchApiServiceTest {

    @Autowired
    private ElasticSearchApiService elasticSearchApiService;
    @Autowired
    private QueryConfigurationProperties queryConfigurationProperties;
    @Autowired
    private ObjectMapper objectMapper;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private ElasticSearchQueryResponse sample;

    private final static String testId = "1";

    @Before
    public void setUp() throws Exception {
        refreshSample();
        if (sample == null) {
            sample = createSampleDocument();
        }
    }

    @Test
    public void hasIndex_givenDefaultIndex_returnsTrue() {
        boolean exist = elasticSearchApiService.hasIndex(queryConfigurationProperties.getDefaultIndex());
        assertThat(exist).isTrue();
    }

    @Test
    public void hasIndex_givenNotExistingDefaultIndex_returnsFalse() throws Exception {
        boolean exist = elasticSearchApiService.hasIndex(UUID.randomUUID().toString());
        assertThat(exist).isFalse();
    }

    @Test
    public void querySearchAll_givenDefaultIndex_returnsANotEmptyList() {
        List<ElasticSearchQueryResponse> responses = elasticSearchApiService.querySearchAll(queryConfigurationProperties.getDefaultIndex());
        assertThat(responses.size()).isGreaterThan(0);
        assertThat(responses.get(0).getIndex()).isEqualTo(queryConfigurationProperties.getDefaultIndex());
        assertThat(responses.get(0).getType()).isNotEmpty();
        assertThat(responses.get(0).getId()).isNotEmpty();
        assertThat(responses.get(0).getSource()).isNotEmpty();
    }

    @Test
    public void querySearchAll_givenANotExistingIndex_returnsAnEmptyList() {
        List<ElasticSearchQueryResponse> responses = elasticSearchApiService.querySearchAll(UUID.randomUUID().toString());
        assertThat(responses.isEmpty()).isTrue();
    }

    @Test
    public void getDocumentById_givenAnIndexAndDocumentId_returnADocument() {
        ElasticSearchQueryResponse document = elasticSearchApiService.getDocumentById(
                queryConfigurationProperties.getDefaultIndex(),
                queryConfigurationProperties.getDefaultType(),
                testId);
        assertThat(document.getSource()).isNotEmpty();
    }

    @Test
    public void getDocumentById_givenAnNotExistingIndexOrDocumentId_returnsNull() {
        ElasticSearchQueryResponse document = elasticSearchApiService.getDocumentById(UUID.randomUUID().toString(), sample.getType(), UUID.randomUUID().toString());
        assertThat(document).isNull();
    }

    @Test
    public void updateDocument_givenAnExistingDocumentId_thenUpdateTheDocument() throws Exception {
        refreshSample();
        JsonNode jsonNode = objectMapper.readTree(sample.getSource());
        long testUid = Instant.now().toEpochMilli();
        ((ObjectNode) jsonNode).put("update_test_uid", testUid);

        String newSource = jsonNode.toString();
        elasticSearchApiService.updateDocument(sample.getIndex(), sample.getType(), sample.getId(), newSource);

        ElasticSearchQueryResponse updatedDocument = elasticSearchApiService.getDocumentById(sample.getIndex(), sample.getType(), sample.getId());
        assertThat(updatedDocument.getSource()).contains("\"update_test_uid\":" + testUid);
    }

    @Test
    public void updateDocument_givenANotExistingDocumentId_thenThrowException() throws Exception {

        expectedException.expect(ElasticsearchStatusException.class);
        expectedException.expectMessage("type=document_missing_exception");
        elasticSearchApiService.updateDocument(sample.getIndex(), sample.getType(), UUID.randomUUID().toString(), "{}");

    }

    @Test
    public void saveDocument_givenAValidDocumentWithoutId_shouldSaveAndReturnANewId() throws Exception {
        refreshSample();

        JsonNode jsonNode = objectMapper.readTree(sample.getSource());
        long testUid = Instant.now().toEpochMilli();
        ((ObjectNode) jsonNode).put("save_test_uid", testUid);
        String newSource = jsonNode.toString();

        ElasticSearchQueryResponse newDoc = elasticSearchApiService.saveDocument(sample.getIndex(), sample.getType(), newSource);
        assertThat(newDoc.getId()).isNotEmpty();
        assertThat(newDoc.getId()).isNotEqualTo(sample.getId());
    }

    @Test
    public void saveDocument_givenAValidDocumentWithANewId_shouldSaveAndReturnTheSameId() throws Exception {

        String newId = "" + Instant.now().toEpochMilli();
        String json = "{\"my_zip\": \"12345\"}";
        ElasticSearchQueryResponse newDoc = elasticSearchApiService.saveDocument(sample.getIndex(), sample.getType(), newId, json);
        assertThat(newDoc.getId()).isNotEmpty();
        assertThat(newDoc.getId()).isEqualTo(newId);
    }

    @Test
    public void saveDocument_givenAValidDocumentWithADuplicateId_shouldOverwriteTheExistingSource() throws Exception {
        refreshSample();
        String newSource = "{\"my_zip\": \"12345\"}";

        ElasticSearchQueryResponse newDoc = elasticSearchApiService.saveDocument(
                sample.getIndex(),
                sample.getType(),
                sample.getId(), newSource);

        assertThat(newDoc.getSource()).isEqualTo(newSource);

    }

    @Test
    public void saveDocument_givenAnInvalidDocument_shouldThrowException() throws Exception {
        refreshSample();
        String invalidJson = "{\"my_zip\": }";

        expectedException.expect(ElasticsearchStatusException.class);
        expectedException.expectMessage("type=mapper_parsing_exception");

        ElasticSearchQueryResponse newDoc = elasticSearchApiService.saveDocument(
                sample.getIndex(),
                sample.getType(),
                sample.getId(), invalidJson);

    }

    @Test
    public void queryMatches_givenParameters_returnMatchedResponses() throws Exception {

        refreshSample();

        JsonNode jsonNode = objectMapper.readTree(sample.getSource());
        long testUid = Instant.now().toEpochMilli();
        ((ObjectNode) jsonNode).put("query_matches_test_uid", testUid);

        String newSource = jsonNode.toString();
        elasticSearchApiService.updateDocument(sample.getIndex(), sample.getType(), sample.getId(), newSource);

        await().atMost(5, SECONDS).untilAsserted(() ->
                assertThat(elasticSearchApiService.queryMatches(sample.getIndex(), "query_matches_test_uid" , testUid)
                        .stream().anyMatch(
                                t -> t.getSource().replaceAll("\\s+","")
                                        .contains("\"query_matches_test_uid\":" + testUid))).isTrue());


    }

    @Test
    public void queryMultiMatches_givenParameters_returnMatchedResponses() throws Exception {
        refreshSample();

        JsonNode jsonNode = objectMapper.readTree(sample.getSource());
        long testUid = Instant.now().toEpochMilli();
        ((ObjectNode) jsonNode).put("query_multi_matche1_test_uid", testUid);
        ((ObjectNode) jsonNode).put("query_multi_matche2_test_uid", testUid);

        String newSource = jsonNode.toString();
        elasticSearchApiService.updateDocument(sample.getIndex(), sample.getType(), sample.getId(), newSource);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("query_multi_matche1_test_uid", testUid);
        parameters.put("query_multi_matche2_test_uid", testUid);

        await().atMost(5, SECONDS).untilAsserted(() ->
                assertThat(elasticSearchApiService.queryMultiMatches(queryConfigurationProperties.getDefaultIndex(), parameters)
                        .stream().anyMatch(
                                t -> t.getSource().replaceAll("\\s+","")
                                        .contains("\"query_multi_matche1_test_uid\":" + testUid) &&
                                        t.getSource().replaceAll("\\s+","")
                                                .contains("\"query_multi_matche2_test_uid\":" + testUid)
                        )).isTrue());

    }

    @Test
    public void upsertTemplate_givenATemplateNameAndSource_shouldStoreAsAsAScript() throws Exception {
        String source = "{\n" +
                "            \"query\": {\n" +
                "                \"bool\": {\n" +
                "                  \"must\": [\n" +
                "                    { \"match\": { \"test_template_save_uid\": {{my_query_uid}} }}\n" +
                "                  ]\n" +
                "                }\n" +
                "\t\t     }\n" +
                "        }";
        elasticSearchApiService.upsertTemplate("test_template_save_uid", source);

        String updatedSource = elasticSearchApiService.getTemplateSource("test_template_save_uid");
        assertThat(updatedSource).isEqualTo(source);
    }

    @Test
    public void query_givenAQueryScript_returnMatchedResponse() throws Exception {
        ElasticSearchQueryResponse sample = elasticSearchApiService.querySearchAll(queryConfigurationProperties.getDefaultIndex()).get(0);

        JsonNode jsonNode = objectMapper.readTree(sample.getSource());
        long testUid = Instant.now().toEpochMilli();
        ((ObjectNode) jsonNode).put("template_query_test_uid", testUid);

        String newSource = jsonNode.toString();
        elasticSearchApiService.updateDocument(sample.getIndex(), sample.getType(), sample.getId(), newSource);

        String source = "{\n" +
                "            \"query\": {\n" +
                "                \"bool\": {\n" +
                "                  \"must\": [\n" +
                "                    { \"match\": { \"template_query_test_uid\": {{my_query_uid}} }}\n" +
                "                  ]\n" +
                "                }\n" +
                "\t\t     }\n" +
                "        }";

        elasticSearchApiService.upsertTemplate("test_template_query_uid", source);

        Map<String, Object> params = new HashMap<>();
        params.put("my_query_uid", testUid);

        await().atMost(5, SECONDS).untilAsserted(() ->
                assertThat(elasticSearchApiService.query(sample.getIndex(), "test_template_query_uid", params)
                        .stream().anyMatch(
                                t -> t.getSource().replaceAll("\\s+","")
                                        .contains("\"template_query_test_uid\":" + testUid)
                        )).isTrue());

    }

    private ElasticSearchQueryResponse createSampleDocument() throws Exception {
        IndexRequest indexRequest = new IndexRequest(
                queryConfigurationProperties.getDefaultIndex(),
                queryConfigurationProperties.getDefaultType(),
                testId)
                .source("test_created", new Date());
        return elasticSearchApiService.saveDocument(indexRequest);
    }

    private void refreshSample() {
        sample = elasticSearchApiService.getDocumentById(
                queryConfigurationProperties.getDefaultIndex(),
                queryConfigurationProperties.getDefaultType(),
                testId);
    }

}