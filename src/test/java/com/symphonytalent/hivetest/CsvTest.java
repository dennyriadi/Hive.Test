package com.symphonytalent.hivetest;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveResource;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.List;



@RunWith(StandaloneHiveRunner.class)
public class CsvTest {
    private List<String> readFile(String path) {
        try {
            return Files.readAllLines(Paths.get(path));
        } catch (IOException ex) {
            System.err.println("Caught IOException: " + ex.getMessage());
            return null;
        }
    }

    private String joinStringCollection(List<String> coll) {
        return String.join("\n", coll);
    }

    private List<Object[]> executeStatementFromFile(HiveShell hiveShell, String filepath) {
        String stmt = joinStringCollection(readFile(filepath)).replace(";", "");
        return hiveShell.executeStatement(stmt);
    }

    @HiveResource(targetFile = "${hiveconf:hadoop.tmp.dir}/profile/1.csv")
    private String profileData = joinStringCollection(readFile("src/test/resources/sampleData/profile_data.csv"));

    @HiveResource(targetFile = "${hiveconf:hadoop.tmp.dir}/interest/1.csv")
    private String interestData = joinStringCollection(readFile("src/test/resources/sampleData/interest_data.csv"));


    @HiveSQL(files = {
            "queries/create_profile_table.hql",
            "queries/create_interest_table.hql",
            "queries/create_profile_interest_table.hql"
    }, autoStart = false)
    private HiveShell hiveShell;

    @Test
    public void testSelectWhere() {
        Object[] expectedResult = new Object[] {
                "fb878fe5-d0d4-4e74-9da6-4bd541aba732",
                "Prospect",
                "2015-03-20 04:32:41.079",
                "0",
                "FALSE"
        };

        hiveShell.start();
        List<Object[]> actualResult = executeStatementFromFile(hiveShell,
                "src/test/resources/queries/simple_select.hql");


        Assert.assertArrayEquals(expectedResult, actualResult.get(0));
    }

    @Test
    public void testGroupByCount() {
        Object[] expectedResult = new Object[] {
                "Accounts",
                3L
        };

        hiveShell.start();
        List<Object[]> actualResult = executeStatementFromFile(hiveShell,
                "src/test/resources/queries/select_count.hql");

        Assert.assertArrayEquals(expectedResult, actualResult.get(0));
    }

    @Test
    public void testJoin() {
        hiveShell.start();
        hiveShell.execute(Paths.get("src/test/resources/queries/insert_join.hql"));

        List<Object[]> result = hiveShell.executeStatement(
                "SELECT id, interestTitle FROM profile_interest WHERE id = 'fb87946e-40d2-401b-8221-b82ad1273df5'");
        Assert.assertEquals(3, result.size());
    }

}
