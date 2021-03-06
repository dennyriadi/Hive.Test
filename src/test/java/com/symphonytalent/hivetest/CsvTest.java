package com.symphonytalent.hivetest;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveResource;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.nio.file.Paths;
import java.util.List;

@RunWith(StandaloneHiveRunner.class)
public class CsvTest {
    @HiveResource(targetFile = "${hiveconf:hadoop.tmp.dir}/profile/1.csv")
    private String profileData = Util.joinStringCollection(Util.readFile("src/test/resources/sampleData/profile_data.csv"));

    @HiveResource(targetFile = "${hiveconf:hadoop.tmp.dir}/interest/1.csv")
    private String interestData = Util.joinStringCollection(Util.readFile("src/test/resources/sampleData/interest_data.csv"));

    @HiveSQL(files = {
            "queries/create_profile_table.hql",
            "queries/create_interest_table.hql",
            "queries/create_profile_interest_table.hql"
    }, autoStart = false)
    private HiveShell hiveShell;

    @Before
    public void start() {
        hiveShell.start();
    }

    @Test
    public void testSelectWhere() {
        Object[] expectedResult = new Object[] {
                "fb878fe5-d0d4-4e74-9da6-4bd541aba732",
                "Prospect",
                "2015-03-20 04:32:41.079",
                "0",
                "FALSE"
        };

        List<Object[]> actualResult = hiveShell.executeStatement(Util.transformQueryFileToStatementString(
            "src/test/resources/queries/simple_select.hql"));

        Assert.assertArrayEquals(expectedResult, actualResult.get(0));
    }

    @Test
    public void testGroupByCount() {
        Object[] expectedResult = new Object[] {
            "Accounts",
            3L
        };

        List<Object[]> actualResult = hiveShell.executeStatement(Util.transformQueryFileToStatementString(
            "src/test/resources/queries/select_count.hql"));

        Assert.assertArrayEquals(expectedResult, actualResult.get(0));
    }

    @Test
    public void testJoin() {
        hiveShell.execute(Paths.get("src/test/resources/queries/insert_join.hql"));

        List<Object[]> result = hiveShell.executeStatement(
                "SELECT id, interestTitle FROM profile_interest WHERE id = 'fb87946e-40d2-401b-8221-b82ad1273df5'");
        Assert.assertEquals(3, result.size());
    }

}
