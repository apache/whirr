package org.apache.whirr.service;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

import java.net.MalformedURLException;

import org.junit.Test;

public class RunUrlBuilderTest {

  private static final String WHIRR_RUNURL_BASE = "whirr.runurl.base";

  @Test
  public void testOnePath() throws MalformedURLException {
    assertThat(runUrls("/a/b"),
        containsString("runurl http://whirr.s3.amazonaws.com/a/b"));
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void testTwoPaths() throws MalformedURLException {
    assertThat(runUrls("/a/b", "/c/d"), allOf(
        containsString("runurl http://whirr.s3.amazonaws.com/a/b"),
        containsString("runurl http://whirr.s3.amazonaws.com/c/d")));
  }

  @Test
  public void testAbsolutePath() throws MalformedURLException {
    assertThat(runUrls("http://example.org/a/b"),
        containsString("runurl http://example.org/a/b"));
  }
  
  @Test
  public void testSystemOverrideOfRunUrlBaseNoSlash() throws MalformedURLException {
    String prev = System.setProperty(WHIRR_RUNURL_BASE, "http://example.org");
    assertThat(runUrls("/a/b"),
        containsString("runurl http://example.org/a/b"));
    if (prev == null) {
      System.clearProperty(WHIRR_RUNURL_BASE);
    } else {
      System.setProperty(WHIRR_RUNURL_BASE, prev);
    }
  }
  
  @Test
  public void testSystemOverrideOfRunUrlBaseWithSlash() throws MalformedURLException {
    String prev = System.setProperty(WHIRR_RUNURL_BASE, "http://example.org/");
    assertThat(runUrls("/a/b"),
        containsString("runurl http://example.org/a/b"));
    if (prev == null) {
      System.clearProperty(WHIRR_RUNURL_BASE);
    } else {
      System.setProperty(WHIRR_RUNURL_BASE, prev);
    }
  }
  
  private String runUrls(String... urls) throws MalformedURLException {
    return new String(RunUrlBuilder.runUrls(urls));
  }
}
