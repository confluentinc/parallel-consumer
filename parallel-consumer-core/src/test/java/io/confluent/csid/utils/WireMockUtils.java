package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;

public class WireMockUtils {

    public static final String stubResponse = "Good times.";

    public WireMockServer setupWireMock() {
        WireMockServer stubServer;
        WireMockConfiguration options = WireMockConfiguration.wireMockConfig().dynamicPort();
        stubServer = new WireMockServer(options);
        MappingBuilder mappingBuilder = WireMock.get(WireMock.urlPathEqualTo("/"))
                .willReturn(WireMock.aResponse()
                        .withBody(stubResponse));
        stubServer.stubFor(mappingBuilder);
        stubServer.stubFor(WireMock.get(WireMock.urlPathEqualTo("/api")).
                willReturn(WireMock.aResponse().withBody(stubResponse)));
        stubServer.start();
        return stubServer;
    }

}
