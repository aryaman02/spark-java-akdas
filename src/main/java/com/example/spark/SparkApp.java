package com.example.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

public class SparkApp {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Simple Spark App").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> cricData = jsc.textFile("/Users/aryaman/spark-apps/spark-java-akdas/src/main/resources/t20.csv");

        JavaRDD<String> playersTeamInfo = cricData.map((Function<String, String>) line -> {
            String[] fields = line.split(",");
            if (fields[1].equals("Player")) {
                return "Foo Bar ()";
            }
            return fields[1];
        });

        JavaRDD<String> teamsPlayersRepresent = playersTeamInfo.flatMap(
                (FlatMapFunction<String, String>) x -> {
                    String teams = x.substring(x.indexOf('(')+1, x.indexOf(')'));
                    List<String> output = new ArrayList<>();

                    if (teams.equals("")) {
                        output.add("N/A");
                    } else if (teams.indexOf('/') == -1) {
                        output.add(teams);
                    } else {
                        String[] arrTeams = teams.split("/");
                        for (String t : arrTeams) {
                            output.add(t);
                        }
                    }
                    return output.iterator();
                });

        List<String> listOfTeams = teamsPlayersRepresent.collect();

        Map<String, Integer> numPlayersTeamMap = new HashMap<>();

        for (String team : listOfTeams) {
            if (!numPlayersTeamMap.containsKey(team)) {
                numPlayersTeamMap.put(team, 1);
            } else {
                numPlayersTeamMap.put(team, numPlayersTeamMap.get(team)+1);
            }
        }

        for (String key : numPlayersTeamMap.keySet()) {
            System.out.println(key + " " + numPlayersTeamMap.get(key).intValue());
        }

        spark.stop();
    }
}