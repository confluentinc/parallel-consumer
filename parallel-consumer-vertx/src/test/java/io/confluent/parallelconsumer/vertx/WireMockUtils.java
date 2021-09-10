package io.confluent.parallelconsumer.vertx;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;

public class WireMockUtils {

    protected static final String stubResponse = "Good times.";

    public WireMockServer setupWireMock() {
        WireMockServer stubServer;
        WireMockConfiguration options = wireMockConfig().dynamicPort();
        stubServer = new WireMockServer(options);
        MappingBuilder mappingBuilder = get(urlPathEqualTo("/"))
                .willReturn(aResponse()
                        .withBody(stubResponse));
        stubServer.stubFor(mappingBuilder);
        stubServer.stubFor(get(urlPathEqualTo("/api")).
                willReturn(aResponse().withBody(stubResponse)));
        stubServer.start();
        return stubServer;
    }

}
