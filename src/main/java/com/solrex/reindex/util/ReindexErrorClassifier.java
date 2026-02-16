package com.solrex.reindex.util;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLException;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;

public final class ReindexErrorClassifier {
    private ReindexErrorClassifier() {
    }

    public static boolean isRetryable(Throwable failure) {
        var root = unwrap(failure);

        if (root instanceof SocketTimeoutException
            || root instanceof ConnectException
            || root instanceof UnknownHostException
            || root instanceof TimeoutException
            || root instanceof InterruptedIOException
            || root instanceof SSLException) {
            return true;
        }

        if (root instanceof IOException) {
            return true;
        }

        if (root instanceof SolrException solrException) {
            var code = solrException.code();
            return code == 429 || code >= 500;
        }

        if (root instanceof SolrServerException serverException && serverException.getCause() != null) {
            return isRetryable(serverException.getCause());
        }

        return false;
    }

    public static Throwable unwrap(Throwable throwable) {
        var current = throwable;

        while (current instanceof CompletionException || current instanceof ExecutionException) {
            if (current.getCause() == null) {
                break;
            }
            current = current.getCause();
        }

        if (current instanceof ReindexStageException stageException && stageException.getCause() != null) {
            return unwrap(stageException.getCause());
        }

        return current;
    }
}
