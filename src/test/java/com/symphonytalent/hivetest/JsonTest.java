package com.symphonytalent.hivetest;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveResource;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.util.List;

@RunWith(StandaloneHiveRunner.class)
public class JsonTest {
    @HiveResource(targetFile = "${hiveconf:hadoop.tmp.dir}/event/1.csv")
    private String eventData = Util.joinStringCollection(Util.readFile("src/test/resources/sampleData/event_data.json"));

    @HiveSQL(files = {
            "queries/create_event_table.hql"
    }, autoStart = false)
    private HiveShell hiveShell;

    @Test
    public void testSelectFromJson() {
        Object[] expectedResult = new Object[] {
            "644e0bef-3d02-4732-9da1-4d7341792afa",
            "fb87946e-40d2-401b-8221-b82ad1273df5"
        };

        hiveShell.start();
        List<Object[]> actualResult = hiveShell.executeStatement("SELECT requestid, profileid FROM event where product = 'pollinatorv3'");

        Assert.assertEquals(1, actualResult.size());
        Assert.assertArrayEquals(expectedResult, actualResult.get(0));
    }
}
