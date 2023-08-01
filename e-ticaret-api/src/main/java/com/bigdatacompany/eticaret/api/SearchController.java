package com.bigdatacompany.eticaret.api;

import jdk.nashorn.internal.ir.RuntimeNode;
import netscape.javascript.JSObject;
import org.json.simple.JSONObject;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@RestController
public class SearchController {
    @GetMapping("search")
    public void searchIndex(@RequestParam String term){
        List<String> cities = Arrays.asList("Ankara", "Istanbul", "Mersin", "Malatya", "Elazığ", "İzmir");
        Random random = new Random();
        int i = random.nextInt(cities.size());
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("search",term);
        jsonObject.put("timestamp",timestamp);
        jsonObject.put("region",cities.get(i));
        System.out.println(jsonObject.toJSONString());
    }
}
