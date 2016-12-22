package com.symphonytalent.hivetest;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveResource;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;



@RunWith(StandaloneHiveRunner.class)
public class PartitionTest {
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

  @HiveResource(targetFile = "${hiveconf:hadoop.tmp.dir}/emailevents/date=2016-12-20/data.json")
  private String eventData1 = joinStringCollection(readFile("src/test/resources/sampleData/emailevents/data1.json"));
  @HiveResource(targetFile = "${hiveconf:hadoop.tmp.dir}/emailevents/date=2016-12-21/data.json")
  private String eventData2 = joinStringCollection(readFile("src/test/resources/sampleData/emailevents/data2.json"));


  @HiveSQL(files = {
          "queries/create_email_event_table.hql"
  }, autoStart = false)
  private HiveShell hiveShell;

  @Before
  public void start() {
    hiveShell.start();
  }

  @Test
  public void testPartitionValidRange() {
    List<Object[]> result = hiveShell.executeStatement(
      "SELECT * FROM email_event WHERE date >= '2016-12-20' AND date <= '2016-12-21'");
    Assert.assertEquals(2, result.size());
  }

  @Test
  public void testPartitionInvalidRange() {
    List<Object[]> result = hiveShell.executeStatement(
      "SELECT * FROM email_event WHERE date >= '2016-10-10' AND date <= '2016-11-13'");
    Assert.assertEquals(0, result.size());
  }
}
