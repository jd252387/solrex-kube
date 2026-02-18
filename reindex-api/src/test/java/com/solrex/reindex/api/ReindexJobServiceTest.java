package com.solrex.reindex.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class ReindexJobServiceTest {
    @Test
    void createUsesClientNamespaceAndReturnsAcceptedResponse() {
        var config = config(Optional.of("ignored-fallback"));
        var clock = Clock.fixed(Instant.parse("2026-02-18T16:25:00Z"), ZoneOffset.UTC);
        var service = new TestableReindexJobService(config, "solrex", clock);

        var response = service.create(TestReindexRequests.valid());

        assertThat(response.status()).isEqualTo("ACCEPTED");
        assertThat(response.jobNamespace()).isEqualTo("solrex");
        assertThat(response.jobName()).matches("reindex-20260218162500-[a-f0-9]{5}");
        assertThat(response.requestConfigMapName()).isEqualTo(response.jobName() + "-request");
        assertThat(response.acceptedAt()).isEqualTo(OffsetDateTime.parse("2026-02-18T16:25:00Z"));

        assertThat(service.namespace).isEqualTo("solrex");
        assertThat(service.configMap.getMetadata().getName()).isEqualTo(response.requestConfigMapName());
        assertThat(service.job.getMetadata().getName()).isEqualTo(response.jobName());
    }

    @Test
    void createFallsBackToConfiguredNamespace() {
        var config = config(Optional.of("solrex"));
        var clock = Clock.fixed(Instant.parse("2026-02-18T16:25:00Z"), ZoneOffset.UTC);
        var service = new TestableReindexJobService(config, null, clock);

        var response = service.create(TestReindexRequests.valid());

        assertThat(response.jobNamespace()).isEqualTo("solrex");
        assertThat(service.namespace).isEqualTo("solrex");
    }

    @Test
    void createThrowsWhenNamespaceIsUnavailable() {
        var config = config(Optional.empty());
        var clock = Clock.fixed(Instant.parse("2026-02-18T16:25:00Z"), ZoneOffset.UTC);
        var service = new TestableReindexJobService(config, "  ", clock);

        assertThatThrownBy(() -> service.create(TestReindexRequests.valid()))
            .isInstanceOf(ReindexJobCreationException.class)
            .hasMessageContaining("namespace is not available");
    }

    @Test
    void createMapsConflictToConflictException() {
        var config = config(Optional.of("solrex"));
        var clock = Clock.fixed(Instant.parse("2026-02-18T16:25:00Z"), ZoneOffset.UTC);
        var service = new TestableReindexJobService(config, "solrex", clock);
        service.throwable = new KubernetesClientException("Conflict", 409, null);

        assertThatThrownBy(() -> service.create(TestReindexRequests.valid()))
            .isInstanceOf(ReindexJobConflictException.class)
            .hasMessageContaining("Generated job name already exists");
    }

    @Test
    void createAlwaysIncludesRequiredEnvironmentVariables() {
        var config = config(Optional.of("solrex"));
        var clock = Clock.fixed(Instant.parse("2026-02-18T16:25:00Z"), ZoneOffset.UTC);
        var service = new TestableReindexJobService(config, "solrex", clock);

        service.create(TestReindexRequests.valid());

        var envNames = service.job.getSpec()
            .getTemplate()
            .getSpec()
            .getContainers()
            .getFirst()
            .getEnv()
            .stream()
            .map(env -> env.getName())
            .toList();

        assertThat(envNames).contains(
            "QUARKUS_KUBERNETES_CONFIG_ENABLED",
            "QUARKUS_KUBERNETES_CONFIG_FAIL_ON_MISSING_CONFIG",
            "REINDEX_K8S_NAMESPACE",
            "REINDEX_CONFIG_MAPS",
            "REINDEX_REQUEST_FILE"
        );
    }

    private static ReindexApiConfig config(Optional<String> fallbackNamespace) {
        var config = mock(ReindexApiConfig.class);
        var k8sConfig = mock(ReindexApiConfig.K8s.class);
        var jobConfig = mock(ReindexApiConfig.Job.class);

        when(config.k8s()).thenReturn(k8sConfig);
        when(config.job()).thenReturn(jobConfig);

        when(k8sConfig.namespace()).thenReturn(fallbackNamespace);

        when(jobConfig.image()).thenReturn("solrex/reindex:latest");
        when(jobConfig.serviceAccount()).thenReturn("reindex-job");
        when(jobConfig.backoffLimit()).thenReturn(1);
        when(jobConfig.ttlSecondsAfterFinished()).thenReturn(3600);
        when(jobConfig.requestFile()).thenReturn("/etc/reindex/request.yaml");
        when(jobConfig.kubernetesConfigEnabled()).thenReturn(true);
        when(jobConfig.kubernetesConfigFailOnMissingConfig()).thenReturn(true);

        return config;
    }

    private static final class TestableReindexJobService extends ReindexJobService {
        private final String currentNamespace;
        private String namespace;
        private ConfigMap configMap;
        private Job job;
        private RuntimeException throwable;

        private TestableReindexJobService(ReindexApiConfig config, String currentNamespace, Clock clock) {
            super(config, mock(KubernetesClient.class), new ReindexJobNameGenerator(), defaultYamlMapper(), clock);
            this.currentNamespace = currentNamespace;
        }

        @Override
        protected String currentNamespace() {
            return currentNamespace;
        }

        @Override
        protected void createResources(String namespace, ConfigMap configMap, Job job) {
            if (throwable != null) {
                throw throwable;
            }
            this.namespace = namespace;
            this.configMap = configMap;
            this.job = job;
        }
    }
}
