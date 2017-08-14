package org.omidbiz.news.crawler;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;

public class JdkHttpConnection
{

    public InputStream sendGetRequest(String url) 
    {
        try
        {
            URL u = new URL(url);
            HttpURLConnection connection = null;
            if (url.startsWith("https"))
                connection = (HttpsURLConnection) u.openConnection();
            else
                connection = (HttpURLConnection) u.openConnection();
            connection.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 5.1; rv:19.0) Gecko/20100101 Firefox/19.0");
            connection.setInstanceFollowRedirects(true);
            connection.setRequestMethod("GET");
            return connection.getInputStream();
            
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return null;
        }
    }
}
