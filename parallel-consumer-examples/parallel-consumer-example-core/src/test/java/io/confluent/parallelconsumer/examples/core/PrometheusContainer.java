package io.confluent.parallelconsumer.examples.core;

/*-
 * Copyright (C) 2020-2022 Confluent, Inc.
 */

import com.github.dockerjava.api.command.InspectContainerResponse;
import lombok.SneakyThrows;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;

public class PrometheusContainer extends GenericContainer<PrometheusContainer> {
    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("prom/prometheus");
    private static final String DEFAULT_TAG = "v2.40.6";
    public static final int PROMETHEUS_PORT = 9090;

    /** @deprecated */
    @Deprecated
    public PrometheusContainer() {
        this(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
    }

    /** @deprecated */
    @Deprecated
    public PrometheusContainer(String prometheusVersion) {
        this(DEFAULT_IMAGE_NAME.withTag(prometheusVersion));
    }

    public PrometheusContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
        dockerImageName.assertCompatibleWith(new DockerImageName[]{DEFAULT_IMAGE_NAME});
        this.withExposedPorts(new Integer[]{PROMETHEUS_PORT});
        this.withAccessToHost(true);
        this.withCopyFileToContainer(
                MountableFile.forHostPath("src/test/resources"),
                "/etc/prometheus");
        this.withReuse(true);
    }

    public String getPrometheusEndpoint() {
        return String.format("http://%s:%s", this.getHost(), this.getMappedPort(PROMETHEUS_PORT));
    }

    @SneakyThrows
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        try {
            ExecResult result = this.execInContainer(new String[]{"nc", "-z", "localhost", "9090"});
            if (result.getExitCode() != 0) {
                throw new IllegalStateException(result.toString());
            }
        } catch (IOException|InterruptedException e) {
            throw e;
        }
    }
}
