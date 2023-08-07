package com.bigdatacompany.eticaret.api;

import com.bigdatacompany.eticaret.MessageProducer;
import jdk.nashorn.internal.ir.RuntimeNode;
import netscape.javascript.JSObject;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@RestController
public class SearchController {
    @Autowired
    MessageProducer messageProducer;
    @GetMapping("search")
    public void searchIndex(@RequestParam String term){
        List<String> cities = Arrays.asList("Ankara", "Istanbul", "Mersin", "Malatya", "Elazığ", "İzmir");
        List<String> products = Arrays.asList("Bebek bezi", "Klavye", "Fare", "Kulaklık", "Temizleme jeli", "Ampul","Koltuk");

        while (true) {
            Random random = new Random();
            int i = random.nextInt(cities.size());
            int k = random.nextInt(products.size());

            long offset = Timestamp.valueOf("2023-07-05 02:00:00").getTime();
            long end = Timestamp.valueOf("2023-07-27 23:50:00").getTime();
            long dif = end - offset + 1;
            Timestamp rand = new Timestamp(offset + (long) (Math.random() * dif));

            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("search", products.get(k));
            jsonObject.put("current_ts", rand.toString());
            jsonObject.put("region", cities.get(i));
            jsonObject.put("userid", random.nextInt(15000-1000)+1000);
            //System.out.println(jsonObject.toJSONString());
            messageProducer.send(jsonObject.toJSONString());
        }
    }

    @GetMapping("/search/stream")
    public void searchIndexStreaming(@RequestParam String term){
        List<String> cities = Arrays.asList("Ankara", "Istanbul", "Mersin", "Malatya", "Elazığ", "İzmir");
        //List<String> products = Arrays.asList("Bebek bezi", "Klavye", "Fare", "Kulaklık", "Temizleme jeli", "Ampul","Koltuk");


            Random random = new Random();
            int i = random.nextInt(cities.size());

            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("search", term);
            jsonObject.put("current_ts", timestamp.toString());
            jsonObject.put("region", cities.get(i));
            jsonObject.put("userid", random.nextInt(15000-1000)+1000);

            messageProducer.send(jsonObject.toJSONString());

    }

}
