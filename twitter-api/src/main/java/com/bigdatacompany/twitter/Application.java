package com.bigdatacompany.twitter;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.List;

public class Application {
    public static void main(String[] args) throws TwitterException {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setOAuthConsumerKey("Ar3CleyzmFeTsuUOY9bZ0EQiT");
        cb.setOAuthConsumerSecret("e6rlNvmsQW7EidNnhw3kYA7rdicpdSWgyDXtC10K5xVMIMZtIj");
        cb.setOAuthAccessToken("293466713-00pYBBygtLarCnP48a74X7PlxLHV6NNXGTb1NYKG");
        cb.setOAuthAccessTokenSecret("iWL1kXBE14hFGWnHG6NeJEVHCX40RlNsQlyytjmQCje0q");

        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter tw = tf.getInstance();
        User umutoku = tw.showUser("doesntgettired");
        String url = umutoku.getURL();
        System.out.println(url);
//     for(Status st: umutoku){
//           System.out.println(st.getText());
       }
    }

