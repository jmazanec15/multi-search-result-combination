/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.searchmerger;

import org.apache.http.util.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;

public class SearchResultMergerPluginIT extends OpenSearchRestTestCase {
    public void testPluginInstalled() throws IOException {
        Response response = client().performRequest(new Request("GET", "/_cat/plugins"));
        String body = EntityUtils.toString(response.getEntity());
        logger.info("response body: {}", body);
    }
}
