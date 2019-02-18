package com.hct.elasticsearch.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ElasticSearchQueryResponse {

    private String id;
    private String index;
    private String type;
    private String source;

}