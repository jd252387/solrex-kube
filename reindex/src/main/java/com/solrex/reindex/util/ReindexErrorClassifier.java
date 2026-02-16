package com.solrex.reindex.util;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLException;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;

public final class ReindexErrorClassifier {
    private ReindexErrorClassifier() {
    }

    public static boolean isRetryable(Throwable failure) {
        var current = failure;
        while (current != null) {
            if (current instanceof ConnectException
                    || current instanceof UnknownHostException
                    || current instanceof TimeoutException
                    || current instanceof InterruptedIOException
                    || current instanceof SSLException) {
                return true;
            }

            if (current instanceof IOException) {
                return true;
            }

            if (current instanceof SolrException solrException) {
                var code = solrException.code();
                if (code == 429 || code >= 500) {
                    return true;
                }
            }

            if (current instanceof SolrServerException serverException && serverException.getCause() == null) {
                return false;
            }

            current = current.getCause();
        }

        return false;
    }
}
