package com.hct.elasticsearch.config.properties;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Configuration
@ConfigurationProperties(prefix = "query")
@Validated
public class QueryConfigurationProperties {


    @NotNull
    private String defaultIndex;
    @NotNull
    private String defaultType;

    @NotNull
    private String storedTemplateId;

}