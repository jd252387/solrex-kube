package com.solrex.reindex.api;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.solrex.reindex.model.ReindexRequest;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import jakarta.inject.Inject;
import jakarta.enterprise.context.ApplicationScoped;
import java.nio.file.Path;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.Objects;

@ApplicationScoped
public class ReindexJobService {
    static final String REQUEST_FILE_KEY = "request.yaml";

    private final ReindexApiConfig config;
    private final KubernetesClient kubernetesClient;
    private final ReindexJobNameGenerator jobNameGenerator;
    private final ObjectMapper yamlObjectMapper;
    private final Clock clock;

    @Inject
    public ReindexJobService(
        ReindexApiConfig config,
        KubernetesClient kubernetesClient,
        ReindexJobNameGenerator jobNameGenerator
    ) {
        this(config, kubernetesClient, jobNameGenerator, defaultYamlMapper(), Clock.systemUTC());
    }

    ReindexJobService(
        ReindexApiConfig config,
        KubernetesClient kubernetesClient,
        ReindexJobNameGenerator jobNameGenerator,
        ObjectMapper yamlObjectMapper,
        Clock clock
    ) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.kubernetesClient = Objects.requireNonNull(kubernetesClient, "kubernetesClient must not be null");
        this.jobNameGenerator = Objects.requireNonNull(jobNameGenerator, "jobNameGenerator must not be null");
        this.yamlObjectMapper = Objects.requireNonNull(yamlObjectMapper, "yamlObjectMapper must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    public CreateReindexJobResponse create(ReindexRequest request) {
        Objects.requireNonNull(request, "request must not be null");

        var namespace = resolveNamespace();
        var jobName = jobNameGenerator.newJobName(clock);
        var configMapName = jobName + "-request";
        var requestYaml = toYaml(request);

        var configMap = buildConfigMap(namespace, configMapName, requestYaml, jobName);
        var job = buildJob(namespace, jobName, configMapName);

        try {
            createResources(namespace, configMap, job);
        } catch (KubernetesClientException e) {
            if (e.getCode() == 409) {
                throw new ReindexJobConflictException("Generated job name already exists: " + jobName, e);
            }
            throw new ReindexJobCreationException("Failed to create reindex resources in Kubernetes.", e);
        }

        return CreateReindexJobResponse.accepted(jobName, namespace, configMapName, OffsetDateTime.now(clock));
    }

    private String resolveNamespace() {
        var currentNamespace = trimToNull(currentNamespace());
        if (currentNamespace != null) {
            return currentNamespace;
        }

        return config.k8s().namespace()
            .map(ReindexJobService::trimToNull)
            .filter(Objects::nonNull)
            .orElseThrow(() -> new ReindexJobCreationException(
                "Kubernetes namespace is not available from client or configuration.",
                null
            ));
    }

    protected String currentNamespace() {
        return kubernetesClient.getNamespace();
    }

    protected void createResources(String namespace, ConfigMap configMap, Job job) {
        kubernetesClient.configMaps()
            .inNamespace(namespace)
            .resource(configMap)
            .create();

        kubernetesClient.batch()
            .v1()
            .jobs()
            .inNamespace(namespace)
            .resource(job)
            .create();
    }

    private String toYaml(ReindexRequest request) {
        try {
            return yamlObjectMapper.writeValueAsString(request);
        } catch (JsonProcessingException e) {
            throw new ReindexJobCreationException("Failed to serialize ReindexRequest to YAML.", e);
        }
    }

    private ConfigMap buildConfigMap(String namespace, String configMapName, String requestYaml, String jobName) {
        return new ConfigMapBuilder()
            .withNewMetadata()
            .withNamespace(namespace)
            .withName(configMapName)
            .addToLabels("app.kubernetes.io/part-of", "solrex")
            .addToLabels("app.kubernetes.io/name", "reindex")
            .addToLabels("solrex.io/reindex-job", jobName)
            .endMetadata()
            .addToData(REQUEST_FILE_KEY, requestYaml)
            .build();
    }

    private Job buildJob(String namespace, String jobName, String requestConfigMapName) {
        var requestPath = RequestFilePath.from(config.job().requestFile());

        return new JobBuilder()
            .withNewMetadata()
            .withName(jobName)
            .withNamespace(namespace)
            .addToLabels("app.kubernetes.io/part-of", "solrex")
            .addToLabels("app.kubernetes.io/name", "reindex")
            .addToLabels("solrex.io/reindex-job", jobName)
            .endMetadata()
            .withNewSpec()
            .withBackoffLimit(config.job().backoffLimit())
            .withTtlSecondsAfterFinished(config.job().ttlSecondsAfterFinished())
            .withNewTemplate()
            .withNewMetadata()
            .addToLabels("solrex.io/reindex-job", jobName)
            .endMetadata()
            .withNewSpec()
            .withServiceAccountName(config.job().serviceAccount())
            .withRestartPolicy("Never")
            .addNewContainer()
            .withName("reindex")
            .withImage(config.job().image())
            .withImagePullPolicy("IfNotPresent")
            .addNewEnv()
            .withName("QUARKUS_KUBERNETES_CONFIG_ENABLED")
            .withValue(Boolean.toString(config.job().kubernetesConfigEnabled()))
            .endEnv()
            .addNewEnv()
            .withName("QUARKUS_KUBERNETES_CONFIG_FAIL_ON_MISSING_CONFIG")
            .withValue(Boolean.toString(config.job().kubernetesConfigFailOnMissingConfig()))
            .endEnv()
            .addNewEnv()
            .withName("REINDEX_K8S_NAMESPACE")
            .withNewValueFrom()
            .withNewFieldRef()
            .withApiVersion("v1")
            .withFieldPath("metadata.namespace")
            .endFieldRef()
            .endValueFrom()
            .endEnv()
            .addNewEnv()
            .withName("REINDEX_CONFIG_MAPS")
            .withValue(requestConfigMapName)
            .endEnv()
            .addNewEnv()
            .withName("REINDEX_REQUEST_FILE")
            .withValue(config.job().requestFile())
            .endEnv()
            .addNewVolumeMount()
            .withName("reindex-request")
            .withMountPath(requestPath.mountDirectory())
            .withReadOnly(true)
            .endVolumeMount()
            .endContainer()
            .addNewVolume()
            .withName("reindex-request")
            .withNewConfigMap()
            .withName(requestConfigMapName)
            .addNewItem()
            .withKey(REQUEST_FILE_KEY)
            .withPath(requestPath.fileName())
            .endItem()
            .endConfigMap()
            .endVolume()
            .endSpec()
            .endTemplate()
            .endSpec()
            .build();
    }

    static ObjectMapper defaultYamlMapper() {
        return new ObjectMapper(new YAMLFactory())
            .findAndRegisterModules()
            .setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE)
            .setVisibility(PropertyAccessor.IS_GETTER, JsonAutoDetect.Visibility.NONE)
            .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        var trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private record RequestFilePath(String mountDirectory, String fileName) {
        private static RequestFilePath from(String configuredPath) {
            var path = Path.of(configuredPath);
            var fileName = path.getFileName() == null ? REQUEST_FILE_KEY : path.getFileName().toString();
            var mountDirectory = path.getParent() == null ? "/etc/reindex" : path.getParent().toString();
            if (mountDirectory.isBlank()) {
                mountDirectory = "/etc/reindex";
            }
            return new RequestFilePath(mountDirectory, fileName);
        }
    }
}
