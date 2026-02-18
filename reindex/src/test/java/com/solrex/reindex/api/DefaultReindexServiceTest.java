package com.solrex.reindex.api;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.solrex.reindex.test.ReindexRequestFixtures;
import com.solrex.reindex.solr.SolrClientFactory;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validation;
import org.junit.jupiter.api.Test;

class DefaultReindexServiceTest {
    @Test
    void shouldUseInjectedValidatorForRequestValidation() {
        var validator = Validation.buildDefaultValidatorFactory().getValidator();
        var service = new DefaultReindexService(new SolrClientFactory(), validator);
        var request = ReindexRequestFixtures.requestWithTargetCollection(" ");

        assertThatThrownBy(() -> service.reindex(request))
            .isInstanceOf(ConstraintViolationException.class);
    }
}
