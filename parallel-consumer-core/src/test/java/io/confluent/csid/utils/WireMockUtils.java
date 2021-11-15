package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.UniformDistribution;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.mockito.BDDMockito.willReturn;

public class WireMockUtils {

    public static final String stubResponse = "Good times.";

    public WireMockServer setupWireMock() {
        WireMockServer stubServer;
        WireMockConfiguration options = WireMockConfiguration.wireMockConfig()
                .dynamicPort()
                .containerThreads(100); // make sure we can respond in parallel
        stubServer = new WireMockServer(options);

        stubServer.stubFor(get(urlPathEqualTo("/"))
                .willReturn(aResponse()
                        .withBody(stubResponse)));

        stubServer.stubFor(get(urlPathEqualTo("/api")).
                willReturn(aResponse()
                        .withBody(stubResponse)));

        stubServer.stubFor(get(urlPathEqualTo("/delay/")).
                willReturn(aResponse()
                        .withFixedDelay(1000)
                        .withBody(stubResponse)));

        stubServer.stubFor(get(urlPathEqualTo("/randomDelay/")).
                willReturn(aResponse()
                        .withRandomDelay(new UniformDistribution(1000, 2000))
                        .withBody(stubResponse)));

        stubServer.stubFor(get(urlPathEqualTo("/error/"))
                .willReturn(aResponse()
                        .withStatus(500)
                        .withRandomDelay(new UniformDistribution(1000, 2000))
                        .withBody(stubResponse)));


        stubServer.start();
        return stubServer;
    }

}
