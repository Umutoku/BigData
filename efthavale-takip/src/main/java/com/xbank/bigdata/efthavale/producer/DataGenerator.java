package com.xbank.bigdata.efthavale.producer;

import org.json.simple.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Timestamp;
import java.util.*;

public class DataGenerator {
    public static List<String> names = new ArrayList<String>();
    public static List<String> surnames = new ArrayList<String>();
    public static Random r = new Random();

    public static int pid = 10000;

    public DataGenerator() throws FileNotFoundException {
        File fileName = new File("C:\\Users\\umuto\\OneDrive\\Masaüstü\\Big Data Uygulamaları Kaynak\\6 - Finans Uygulamaları\\isimler.txt");
        File fileSurname = new File("C:\\Users\\umuto\\OneDrive\\Masaüstü\\Big Data Uygulamaları Kaynak\\6 - Finans Uygulamaları\\soyisimler.txt");

        Scanner fileNameScanner = new Scanner(fileName);
        Scanner fileSurNameScanner = new Scanner(fileSurname);

        while (fileNameScanner.hasNext()){
            names.add(fileNameScanner.nextLine());
        }

        while (fileSurNameScanner.hasNext()){
            surnames.add(fileSurNameScanner.nextLine());
        }

    }
    public String generate() {

        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        List<String> btype = Arrays.asList("TL", "USD", "EUR");


            JSONObject data = new JSONObject();

            data.put("pid",pid++);
            data.put("ptype","H");

            JSONObject account = new JSONObject();
            account.put("oid",generateID());
            account.put("title",nameSurnameGenerator());
            account.put("iban","TR"+generateID());

            data.put("account",account);

            JSONObject info = new JSONObject();
            info.put("title",nameSurnameGenerator());
            info.put("iban","TR"+generateID());
            info.put("bank","X Bank");

            data.put("info",info);

            data.put("balance",r.nextInt(99999-0)+0);
            data.put("btype",btype.get(r.nextInt(btype.size())));
            data.put("current_ts",timestamp.toString());

            return data.toJSONString();

    }
    public static long generateID(){
        long numbers = 1000000000L + (long)(r.nextDouble()*9999999999L);
        return numbers;
    }

    public static String nameSurnameGenerator(){
        String name = names.get(r.nextInt(names.size()));
        String surname = surnames.get(r.nextInt(surnames.size()));

        return name + " " + surname;
    }
}
