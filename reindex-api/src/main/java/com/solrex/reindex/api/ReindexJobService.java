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
import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.Objects;

@ApplicationScoped
public class ReindexJobService {
    private static final String LABEL_PART_OF = "app.kubernetes.io/part-of";
    private static final String LABEL_NAME = "app.kubernetes.io/name";
    private static final String LABEL_REINDEX_JOB = "solrex.io/reindex-job";
    private static final String LABEL_PART_OF_VALUE = "solrex";
    private static final String LABEL_NAME_VALUE = "reindex";
    private static final String REINDEX_CONTAINER_NAME = "reindex";
    private static final String ENV_QUARKUS_KUBERNETES_CONFIG_ENABLED = "QUARKUS_KUBERNETES_CONFIG_ENABLED";
    private static final String ENV_QUARKUS_KUBERNETES_CONFIG_FAIL_ON_MISSING_CONFIG =
        "QUARKUS_KUBERNETES_CONFIG_FAIL_ON_MISSING_CONFIG";
    private static final String ENV_REINDEX_K8S_NAMESPACE = "REINDEX_K8S_NAMESPACE";
    private static final String ENV_REINDEX_CONFIG_MAPS = "REINDEX_CONFIG_MAPS";
    static final String REQUEST_CONFIG_KEY = "reindex.job.request";

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
            .addToLabels(LABEL_PART_OF, LABEL_PART_OF_VALUE)
            .addToLabels(LABEL_NAME, LABEL_NAME_VALUE)
            .addToLabels(LABEL_REINDEX_JOB, jobName)
            .endMetadata()
            .addToData(REQUEST_CONFIG_KEY, requestYaml)
            .build();
    }

    private Job buildJob(String namespace, String jobName, String requestConfigMapName) {
        return new JobBuilder()
            .withNewMetadata()
            .withName(jobName)
            .withNamespace(namespace)
            .addToLabels(LABEL_PART_OF, LABEL_PART_OF_VALUE)
            .addToLabels(LABEL_NAME, LABEL_NAME_VALUE)
            .addToLabels(LABEL_REINDEX_JOB, jobName)
            .endMetadata()
            .withNewSpec()
            .withBackoffLimit(config.job().backoffLimit())
            .withTtlSecondsAfterFinished(config.job().ttlSecondsAfterFinished())
            .withNewTemplate()
            .withNewMetadata()
            .addToLabels(LABEL_REINDEX_JOB, jobName)
            .endMetadata()
            .withNewSpec()
            .withServiceAccountName(config.job().serviceAccount())
            .withRestartPolicy("Never")
            .addNewContainer()
            .withName(REINDEX_CONTAINER_NAME)
            .withImage(config.job().image())
            .withImagePullPolicy("IfNotPresent")
            .addNewEnv()
            .withName(ENV_QUARKUS_KUBERNETES_CONFIG_ENABLED)
            .withValue(Boolean.toString(config.job().kubernetesConfigEnabled()))
            .endEnv()
            .addNewEnv()
            .withName(ENV_QUARKUS_KUBERNETES_CONFIG_FAIL_ON_MISSING_CONFIG)
            .withValue(Boolean.toString(config.job().kubernetesConfigFailOnMissingConfig()))
            .endEnv()
            .addNewEnv()
            .withName(ENV_REINDEX_K8S_NAMESPACE)
            .withNewValueFrom()
            .withNewFieldRef()
            .withApiVersion("v1")
            .withFieldPath("metadata.namespace")
            .endFieldRef()
            .endValueFrom()
            .endEnv()
            .addNewEnv()
            .withName(ENV_REINDEX_CONFIG_MAPS)
            .withValue(requestConfigMapName)
            .endEnv()
            .endContainer()
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
}
