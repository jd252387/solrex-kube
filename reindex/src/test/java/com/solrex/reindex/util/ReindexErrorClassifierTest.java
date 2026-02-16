package com.solrex.reindex.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.ConnectException;
import java.util.concurrent.CompletionException;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.junit.jupiter.api.Test;

class ReindexErrorClassifierTest {
    @Test
    void shouldClassifyRetryableWhenSolrjFailureIsNestedInWrapperExceptions() {
        var failure = new CompletionException(new SolrServerException("request failed", new ConnectException("down")));

        assertThat(ReindexErrorClassifier.isRetryable(failure)).isTrue();
    }

    @Test
    void shouldClassifyRetryableForSolrTooManyRequests() {
        var failure = new SolrException(SolrException.ErrorCode.TOO_MANY_REQUESTS, "busy");

        assertThat(ReindexErrorClassifier.isRetryable(failure)).isTrue();
    }

    @Test
    void shouldNotClassifyRetryableForSolrBadRequest() {
        var failure = new SolrException(SolrException.ErrorCode.BAD_REQUEST, "bad request");

        assertThat(ReindexErrorClassifier.isRetryable(failure)).isFalse();
    }
}
