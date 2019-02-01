/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").  See License in the project root for license information.
 */

package com.linkedin.kafka.clients.metadataservice;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import javax.net.ssl.SSLContext;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.http.Consts.UTF_8;


// A generic HTTP client for a REST service.
public class RestClient {
  /**
   * A wrapper for HTTP status and response content.
   */
  public static class HttpResult {
    private final int _statusCode;
    private final String _responseContent;

    HttpResult(int statusCode, String responseContent) {
      _statusCode = statusCode;
      _responseContent = responseContent;
    }

    public int statusCode() {
      return _statusCode;
    }

    public String responseContent() {
      return _responseContent;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(RestClient.class);
  private static final int DEFAULT_HTTP_CLIENT_TIMEOUT_MS = 1000;

  private final String _serviceUri;
  private final CloseableHttpClient _httpClient;

  public RestClient(String serviceUri) {
    this(serviceUri, null, DEFAULT_HTTP_CLIENT_TIMEOUT_MS);
  }

  public RestClient(String serviceUri, SSLContext sslContext) {
    this(serviceUri, sslContext, DEFAULT_HTTP_CLIENT_TIMEOUT_MS);
  }

  public RestClient(String serviceUri, SSLContext sslContext, int connectionTimeoutMs) {
    _serviceUri = serviceUri;

    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(connectionTimeoutMs)
        .setSocketTimeout(connectionTimeoutMs)
        .setConnectionRequestTimeout(connectionTimeoutMs).build();
    _httpClient = HttpClients.custom()
        .setDefaultRequestConfig(requestConfig)
        .setConnectionManager(new PoolingHttpClientConnectionManager())
        .setSSLContext(sslContext)
        .build();
  }

  public HttpResult doGet(String path) throws IOException, URISyntaxException {
    URI uri = null;
    HttpGet httpGet = null;
    try {
      uri = new URIBuilder(_serviceUri).setPath(path).build();
      httpGet = new HttpGet(uri);
      try (CloseableHttpResponse httpResponse = _httpClient.execute(httpGet)) {
        return getResult(httpResponse);
      }
    } catch (Exception e) {
      LOG.error("HTTP GET to URI {} failed: {}", uri, httpGet);
      throw e;
    }
  }

  public HttpResult doPost(String path, List<NameValuePair> postParameters) throws IOException, URISyntaxException {
    URI uri = null;
    HttpPost httpPost = null;
    try {
      uri = new URIBuilder(_serviceUri).setPath(path).build();
      httpPost = new HttpPost(uri);
      httpPost.setEntity(new UrlEncodedFormEntity(postParameters, UTF_8));
      try (CloseableHttpResponse httpResponse = _httpClient.execute(httpPost)) {
        return getResult(httpResponse);
      }
    } catch (Exception e) {
      LOG.error("HTTP POST to URI {} failed: {}", uri, httpPost);
      throw e;
    }
  }

  private HttpResult getResult(CloseableHttpResponse httpResponse) throws IOException {
    int statusCode = httpResponse.getStatusLine().getStatusCode();
    HttpEntity entity = httpResponse.getEntity();
    String responseContent = EntityUtils.toString(entity);
    // Ensure the response is fully consumed, which in turn ensures the connection is released and ready for reuse.
    EntityUtils.consume(entity);
    return new HttpResult(statusCode, responseContent);
  }
}