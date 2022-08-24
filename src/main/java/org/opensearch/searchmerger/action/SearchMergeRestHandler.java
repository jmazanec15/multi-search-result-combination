package org.opensearch.searchmerger.action;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.MultiSearchAction;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestCancellableNodeClient;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.SearchSourceBuilder;


import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.common.xcontent.support.XContentMapValues.nodeStringArrayValue;

public class SearchMergeRestHandler extends BaseRestHandler {

    private static final int TOTAL_HITS = 5;

    private static final Logger logger = LogManager.getLogger(SearchMergeRestHandler.class);

    private final static String NAME = "search_merger_action";
    private static final String SEARCH = "_search_merge";

    public static final String SEARCH_MERGE_BASE_URI = "/_plugins/_smerge";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<Route> routes() {
        return ImmutableList.of(
                new Route(RestRequest.Method.GET, String.format(Locale.ROOT, "%s/%s", SEARCH_MERGE_BASE_URI, SEARCH)),
                new Route(RestRequest.Method.POST, String.format(Locale.ROOT, "%s/%s", SEARCH_MERGE_BASE_URI, SEARCH))
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient) throws IOException {
        final MultiSearchRequest multiSearchRequest = parseMultiSearchRequest(restRequest, nodeClient.getNamedWriteableRegistry(), nodeClient);


        return channel -> {
            final RestCancellableNodeClient cancellableClient = new RestCancellableNodeClient(nodeClient, restRequest.getHttpChannel());
            cancellableClient.execute(MultiSearchAction.INSTANCE, multiSearchRequest, ActionListener.wrap(multiSearchResponse -> {
                MultiSearchResponse.Item[] searchResponses = multiSearchResponse.getResponses();

                int totalHits = 0;
                float maxScore = 0.0f;
                SearchHit[] searchHitArray = new SearchHit[TOTAL_HITS];

                int queries = searchResponses.length;

                for (int i = 0; i < TOTAL_HITS; i++) {
                    searchHitArray[i] = searchResponses[i % queries].getResponse().getHits().getAt(i / queries);
                }

                PriorityQueue<SearchHit> priorityQueue = new PriorityQueue<>(TOTAL_HITS, (o1, o2) -> Float.compare(o1.getScore(), o2.getScore()));
                for (MultiSearchResponse.Item item : searchResponses) {
                    for (SearchHit searchHit : item.getResponse().getHits()) {

                        if (searchHit.getScore() > maxScore) {
                            maxScore = searchHit.getScore();
                        }

                        totalHits += 1;

                        priorityQueue.add(searchHit);
                    }
                }

//                int i = 0;
//                while (!priorityQueue.isEmpty()) {
//                    searchHitArray[i++] = priorityQueue.poll();
//                }

                SearchHits searchHits = new SearchHits(searchHitArray, new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), maxScore);
                SearchResponseSections searchResponseSections = new SearchResponseSections(searchHits,null, null, false, false, null, 10);
                SearchResponse searchResponse = new SearchResponse(searchResponseSections, null, 10, 10, 0, 10, new ShardSearchFailure[0], SearchResponse.Clusters.EMPTY);
                new RestToXContentListener<>(channel).onResponse(searchResponse);
            }, e -> new RestToXContentListener<>(channel).onFailure(e)));
        };
    }

    private MultiSearchRequest parseMultiSearchRequest(RestRequest restRequest, NamedWriteableRegistry namedWriteableRegistry, Client client) throws IOException {

        MultiSearchRequest multiRequest = new MultiSearchRequest();

        XContentParser parser = restRequest.contentParser();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);

        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            if ("queries".equals(fieldName)) {
                parseQueries(multiRequest, parser, client);
            } else {
                throw new IllegalStateException("field wasnt queries");
            }
        }

        return multiRequest;
    }

    private void parseQueries(MultiSearchRequest multiSearchRequest, XContentParser parser, Client client) throws IOException {

        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new IllegalStateException("Expected an array but got something else");
        }
        parser.nextToken();
        SearchRequestBuilder searchRequestBuilder = null;
        while (parser.currentToken() != XContentParser.Token.END_ARRAY) {
            parser.nextToken();
            String fieldName = parser.currentName();
            parser.nextToken();

            if ("meta".equals(fieldName)) {
                if (searchRequestBuilder != null) {
                    throw new IllegalStateException("Search request builder is not null when it should be");
                }
                searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE);
                parseMeta(searchRequestBuilder, parser);
                continue;
            }

            if ("body".equals(fieldName)) {
                if (searchRequestBuilder == null) {
                    throw new IllegalStateException("Search request builder is null when it shouldn't be");
                }
                parseBody(searchRequestBuilder, parser);
                multiSearchRequest.add(searchRequestBuilder);
                searchRequestBuilder = null;
            }
        }
    }

    private void parseMeta(SearchRequestBuilder searchRequestBuilder, XContentParser parser) throws IOException {
        Map<String, Object> source = parser.map();
        for (Map.Entry<String, Object> entry : source.entrySet()) {
            Object value = entry.getValue();
            if ("index".equals(entry.getKey()) || "indices".equals(entry.getKey())) {
                searchRequestBuilder.setIndices(nodeStringArrayValue(value));
            } else {
                throw new IllegalStateException("Invalid meta data field");
            }
        }
    }

    private void parseBody(SearchRequestBuilder searchRequestBuilder, XContentParser parser) throws IOException {
        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser, false);
        searchRequestBuilder.setSource(searchSourceBuilder);
    }
}
