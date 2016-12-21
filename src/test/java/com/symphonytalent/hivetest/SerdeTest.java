package com.symphonytalent.hivetest;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveResource;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


@RunWith(StandaloneHiveRunner.class)
public class SerdeTest {
    private final String hdfsSource = "${hiveconf:hadoop.tmp.dir}/customSerde/data.csv";

    @HiveResource(targetFile = "${hiveconf:hadoop.tmp.dir}/customSerde/data.csv")
    private String data = "" +
            "1,James\n" +
            "2,John\n" +
            "3,Charles\n";


    @HiveSQL(files = {"queries/create_table.sql"}, autoStart = false)
    private HiveShell hiveShell;

    @Test
    public void testWithProvidedRegexSerde() {
        hiveShell.start();
        List<String> actualResult = hiveShell.executeQuery(Paths.get("src/test/resources/queries/simple_select.sql"));

        Assert.assertEquals(Arrays.asList("1\tJames"),  actualResult);
    }
//
//    @Test
//    public void testWithCustomSerde() throws TException, IOException {
//        hiveShell.start();
//        List<String> actual = hiveShell.executeQuery(String.format("select * from customSerdeTable"));
//        List<String> expected = Arrays.asList(
//                "Q\tW\tE",
//                "R\tT\tY",
//                "U\tI\tO",
//                "A\tB\tC",
//                "F\tG\tH",
//                "T\tJ\tK");
//
//        Collections.sort(actual);
//        Collections.sort(expected);
//
//        Assert.assertEquals(expected, actual);
//    }


}