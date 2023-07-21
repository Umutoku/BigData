package com.bigdatacompany.elastic;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpHost;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.List;

public class Application {

    private static final Logger logger = LogManager.getLogger(Application.class);

    public static void main(String[] args) throws IOException {
        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 9200, "http"),
                new HttpHost("localhost", 9201, "http")).build();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // And create the API client
        ElasticsearchClient esClient = new ElasticsearchClient(transport);


        String searchText = "5";

        SearchResponse<ObjectNode> response = esClient.search(s -> s
                        .index("exam")
                        .query(q -> q
                                .match(t -> t
                                        .field("name")
                                        .query(searchText)
                                )
                        ),
                ObjectNode.class
        );
        System.out.println(response.hits());

        TotalHits total = response.hits().total();
        assert total != null;
        boolean isExactResult = total.relation() == TotalHitsRelation.Eq;

        extracted(total, isExactResult);

//        Map<String,Object> map = new HashMap<>();
//        map.put("name","asus");
//        map.put("detail","e15 vivobook");
//        map.put("price","5000");
//        map.put("provider","asus Türkiye");
//
//        IndexRequest<Map<String, Object>> request = IndexRequest.of(i -> i
//                .index("exam").document(map)
//
//        );
//
//        IndexResponse responsee = esClient.index(request);
//        System.out.println(responsee.id());

        GetResponse<ObjectNode> responseee = esClient.get(g -> g
                        .index("exam")
                        .id("DFbaRIkB_iAFmZV1S79x"),
                ObjectNode.class
        );

        if (responseee.found()) {
            ObjectNode json = responseee.source();
            assert json != null;
            String name = json.get("name").asText();
            String provider = json.get("provider").asText();
            System.out.println("Product name: " + name + " Provider: " + provider);
        }

             SearchResponse<ObjectNode> responseSearch = esClient.search(s -> s
                        .index("exam")
                        .query(q -> q
                                .match(t -> t
                                        .field("name")
                                        .query("asus")
                                )
                        ),
                ObjectNode.class
        );

        TotalHits totalSearch = responseSearch.hits().total();
        assert totalSearch != null;
        boolean isExactResultSearch = totalSearch.relation() == TotalHitsRelation.Eq;

        BasicConfigurator.configure();

        if (isExactResultSearch) {
            logger.info("There are " + total.value() + " results");
        } else {
            logger.info("There are more than " + total.value() + " results");

        }

        List<Hit<ObjectNode>> hits = responseSearch.hits().hits();
        for (Hit<ObjectNode> hit: hits) {
            ObjectNode objectNode = hit.source();
            assert objectNode != null;
            logger.info("Found product " + objectNode.get("name") + ", score " + hit.score());
        }

        DeleteRequest request = DeleteRequest.of(d -> d.index("exam").id("100"));
        DeleteResponse responseDelete = esClient.delete(request);
        System.out.println(responseDelete);

        DeleteByQueryRequest dbyQuery = DeleteByQueryRequest
                .of(fn -> fn.query(TermQuery.of(tq -> tq.field("provider").value("Asus")).
                        _toQuery()).index("exam"));

        DeleteByQueryResponse dqr = esClient.deleteByQuery(dbyQuery);
        System.out.println(dqr.deleted());
    }

    private static void extracted(TotalHits total, boolean isExactResult) {
        if (isExactResult) {
            System.out.println("There are " + total.value() + " results");
        } else {
            System.out.println("There are more than " + total.value() + " results");
        }
    }
}
