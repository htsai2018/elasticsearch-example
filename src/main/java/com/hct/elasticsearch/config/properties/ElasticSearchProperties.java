package com.hct.elasticsearch.config.properties;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Configuration
@ConfigurationProperties(prefix = "elasticsearch")
@Validated
public class ElasticSearchProperties {

    @NotEmpty
    private String host;
    @NotEmpty
    private String scheme;

    @NotNull
    private int port;
    @NotEmpty
    private String username;
    @NotEmpty
    private String password;
}