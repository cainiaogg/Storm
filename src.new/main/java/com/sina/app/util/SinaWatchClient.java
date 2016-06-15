package com.sina.app.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

/**
 * This file comes from http://wiki.intra.sina.com.cn/pages/viewpage.action?pageId=7162903.
 *
 */
public class SinaWatchClient {

    public String kid = "put your kid";
    public String passwd = "put your password";
    public boolean auth = true;

    public String host = "connect.monitor.sina.com.cn";
    public int port = 80;

    public SinaWatchClient() {
    }

    SinaWatchClient(String host, int port) {
        this.host = host;
        this.port = port;
    }
    
    public String md5(String body) {
        char hexDigits[]={'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
        try {
            byte[] btInput = body.getBytes();
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            mdInst.update(btInput);
            byte[] md = mdInst.digest();
            int j = md.length;
            char str[] = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++) {
                byte byte0 = md[i];
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];
                str[k++] = hexDigits[byte0 & 0xf];
            }
            return new String(str).toLowerCase();
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    private void hash(HttpRequestBase request) throws ParseException, IOException {
        // content-type
        String content_type = "application/x-www-form-urlencoded";
        Header[] headers = request.getHeaders("Content-Type");
        if (headers.length > 0) {
            content_type = headers[0].getValue();
        } else {
            request.addHeader("Content-Type",content_type);
        }
        
        // md5
        String content_md5 = "";
        if (request instanceof HttpPost) {
            HttpEntity entity = ((HttpPost) request).getEntity();
            String body = EntityUtils.toString(entity);
//            System.out.println(body);
            
            content_md5 = this.md5(body);
            request.addHeader("Content-MD5", content_md5);
        }

        // expire
        long expire_time = new Date().getTime() / 60 + 60 * 10;
        request.addHeader("Expires", expire_time + "");

        // hash
        String method = request.getRequestLine().getMethod().toUpperCase();

        // hard coded, make my life easier...
        String canonicalizedamzheaders = "";
        String canonicalizedresource = request.getURI().getPath();
        if (request.getURI().getQuery() != null && request.getURI().getQuery().length() > 0) {
            canonicalizedresource += "?" + request.getURI().getQuery();
        }
        
        String stringToSign =  method + "\n" +
                content_md5 + "\n" +
                content_type + "\n" +
                expire_time + "\n" +
                canonicalizedamzheaders + canonicalizedresource;
        
        String ssig = this.sign(stringToSign, this.passwd);
        request.addHeader("Authorization", "sinawatch "+ this.kid +":" + ssig);
    }
    
    private String sign(String stringToSign, String passwd) {
        String algorithm = "HmacSHA1";
        try {
            Mac mac = Mac.getInstance(algorithm);
            mac.init(new SecretKeySpec(passwd.getBytes("UTF-8"), algorithm));
            byte[] signature = mac.doFinal(stringToSign.getBytes("UTF-8"));
            signature = Base64.encodeBase64(signature);
        
            return new String(signature).substring(5, 15);
        } catch (Exception e) {
            e.printStackTrace();
            // no way to raise exception here.
            return "";
        }
    }

    public void bind_auth(String kid, String passwd) {
        this.kid = kid;
        this.passwd = passwd;
    }

    public String get(String url, List<NameValuePair> parameters)
        throws IOException, java.net.URISyntaxException {
        
        URIBuilder builder = new URIBuilder()
        .setScheme("http")
        .setHost(this.host)
        .setPort(this.port)
        .setPath(url);
        
        Iterator<NameValuePair> iterator = parameters.iterator();
        while (iterator.hasNext()) {
            NameValuePair kv = iterator.next();
            builder.setParameter(kv.getName(), kv.getValue());
        }
        
        URI uri = builder.build();
        HttpGet request = new HttpGet(uri);
        return this.fetch(request);
    }

    public String post(String url, List<NameValuePair> parameters)
        throws IOException, URISyntaxException {
        
        URI uri = new URIBuilder()
        .setScheme("http")
        .setHost(this.host)
        .setPort(this.port)
        .setPath(url)
        .build();
        
        HttpPost request = new HttpPost(uri);

        HttpEntity entity = new UrlEncodedFormEntity(parameters, "UTF-8");
        request.setEntity(entity);
        
        return this.fetch(request);
    }

    public String post(String url, String jsonized)
        throws IOException, URISyntaxException {
        
        URI uri = new URIBuilder()
        .setScheme("http")
        .setHost(this.host)
        .setPort(this.port)
        .setPath(url)
        .build();
        
        HttpPost request = new HttpPost(uri);

        HttpEntity entity = new StringEntity(jsonized);
        request.setEntity(entity);
        
        return this.fetch(request);
    }
    
    public String fetch(HttpRequestBase request) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {
            if (this.auth) {
                this.hash(request);
            }
            
            // Create a response handler
            ResponseHandler<String> responseHandler = new ResponseHandler<String>() {
                public String handleResponse(final HttpResponse response) throws ClientProtocolException, IOException {
                    int status = response.getStatusLine().getStatusCode();
                    if (status == 200) {
                        HttpEntity entity = response.getEntity();
                        return entity != null ? EntityUtils.toString(entity) : null;
                    } else {
                        throw new ClientProtocolException("Unexpected response status: " + status);
                    }
                }
            };

            String responseBody = httpclient.execute(request, responseHandler);
            return responseBody;
        } finally {
            httpclient.close();
        }
    }

    public static void main(String[] args) {
        // http://hc.apache.org/httpcomponents-client-ga/tutorial/html/index.html
        SinaWatchClient client = new SinaWatchClient();

        client.bind_auth("2012101713", "XCSN1h7cywzcBwtOA2MndonTnLLT7R");

        try {
            List<NameValuePair> parameters = new ArrayList<NameValuePair>();
            parameters.add(new BasicNameValuePair("sv", "门户广告"));
            parameters.add(new BasicNameValuePair("service", "test"));
            parameters.add(new BasicNameValuePair("subject", "test"));
            parameters.add(new BasicNameValuePair("object", "test"));
            parameters.add(new BasicNameValuePair("content", "content of alert"));
            parameters.add(new BasicNameValuePair("mailto", "xiaocheng1,lijun19"));

            String response = client.post("/v1/alert/send", parameters);
            System.out.println(response);

//            response = client.post("/v1/stream/metric/push", parameters);
//            System.out.println(response);
//            
//            response = client.get("/v1/stream/metric/query", parameters);
//            System.out.println(response);
        } catch (Exception e) {
              e.printStackTrace();
        }
    }
}

