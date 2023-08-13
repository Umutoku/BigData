package com.xcompany.bigdata.search.service;

import com.google.gson.Gson;
import com.xcompany.bigdata.search.model.AutoCompleteDetail;
import com.xcompany.bigdata.search.model.AutocompleteResponse;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Service;
import static com.xcompany.bigdata.search.model.Constants.*;
import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class AutocompleteServiceImpl implements AutocompleteService {

    RestHighLevelClient client;
    SearchSourceBuilder builder;
    SearchRequest request;
    Gson gson;
    @PostConstruct
    public void init(){
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(HOSTNAME,PORT)));
         builder= new SearchSourceBuilder();

    }
    @Override
    public AutocompleteResponse search(String term) throws IOException {
        List<AutoCompleteDetail> data = new ArrayList<AutoCompleteDetail>();
        builder.query(QueryBuilders.matchQuery(AUTOCOMPLETE,term).fuzziness(1));
         request = new SearchRequest(INDEX).source(builder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHit[] hits = response.getHits().getHits();
        for(int i=0 ;i<hits.length;i++)
        {
            String responseDetail = hits[i].getSourceAsString();
            AutoCompleteDetail detail = gson.fromJson(responseDetail, AutoCompleteDetail.class);
            data.add(detail);
        }
        return new AutocompleteResponse(data);
    }
}
