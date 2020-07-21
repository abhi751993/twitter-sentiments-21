/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> twitterDataStream = getTwitterDataStream(env);

        DataStream<Tuple2<String, Integer>> wordCountDataStream = getWordCountStream(twitterDataStream);

        wordCountDataStream.keyBy(0).sum(1).print();
        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }

    private static DataStream<Tuple2<String, Integer>> getWordCountStream(DataStream<String> twitterDataStream) {
        return twitterDataStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

                    private transient ObjectMapper jsonParser;

                    @Override
                    public void flatMap(String tweetString, Collector<Tuple2<String, Integer>> collector) throws Exception {

                        if (jsonParser == null) {
                            jsonParser = new ObjectMapper();
                        }

                        JsonNode tweetJson = jsonParser.readValue(tweetString, JsonNode.class);

                        boolean isEnglish = isTweetInEnglish(tweetJson);
                        boolean hasText = tweetJson.has("text");

//                        System.out.println(isEnglish && hasText);

                        if (hasText)
                            System.out.println(tweetJson.get("text").asText("asd"));

                        if (isEnglish && hasText) {
                            System.out.println(tweetString);
                            String[] tokens = tweetJson.get("text").asText().split("\\s*");

                            for (String token : tokens) {
                                System.out.println(token);
                                collector.collect(new Tuple2<String, Integer>(token, 1));
                            }
                        }

                    }

                    private boolean isTweetInEnglish(JsonNode tweetJson) {
                        return tweetJson.has("user") &&
                                tweetJson.get("user").has("lang") &&
                                (tweetJson.get("user").get("lang").asText().compareTo("en") == 0);
                    }
                });
    }

    private static DataStream<String> getTwitterDataStream(StreamExecutionEnvironment env) {
        Properties props = new Properties();
        props.setProperty(TwitterSource.CONSUMER_KEY, "sKOGV2w91eEm2ZlKZVN6gfGGc");
        props.setProperty(TwitterSource.CONSUMER_SECRET, "os2MhCVqDOW7cUeUFa6pfXKBpYVkD8d8Ual3bx3rCZN666Vbd4");
        props.setProperty(TwitterSource.TOKEN, "3018010825-pVuwORHW51LdOA7N8dnTZVf1IsuXiSDMKyTxyPq");
        props.setProperty(TwitterSource.TOKEN_SECRET, "HZmvhKVq6wzvLOM0uYGQAVlfJIRHfyHKIcpUk0plsYcmK");
        return env.addSource(new TwitterSource(props));
    }

}
