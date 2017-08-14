package org.omidbiz.news.crawler;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URLEncoder;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.sax.BodyContentHandler;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.xml.sax.SAXException;

/**
 * @author Omid Pourhadi
 *
 */
public class VncVerticle extends AbstractVerticle
{

    private static final Logger LOGGER = LoggerFactory.getLogger(VncVerticle.class);

    private static final String SQL_CREATE_NEWS_TABLE = "create table if not exists News (Id integer identity primary key, News_Id integer unique, Lang varchar(255), Keywords varchar(255) , content clob, raw_content clob, Metadata clob)";

    JDBCClient jdbcClient;
    WebClient webClient;

    @Override
    public void start(Future<Void> startFuture) throws Exception
    {
        WebClientOptions options = new WebClientOptions().setUserAgent("Mozilla");
        options.setKeepAlive(false);
        webClient = WebClient.create(vertx, options);
        Future<Void> prepareDatabase = prepareDatabase();
        Future<Void> c = startCrawler();
        c.setHandler(prepareDatabase.completer());
        prepareDatabase.setHandler(handler -> {
            startFuture.complete();
        });

    }

    private Future<Void> prepareDatabase()
    {
        LOGGER.info("PREPATE DATABASE");
        LOGGER.info("=======================");
        Future<Void> future = Future.future();
        JsonObject config = new JsonObject().put("url", "jdbc:h2:~/newsdb").put("driver_class", "org.h2.Driver").put("max_idle_time", 300)
                .put("provider_class", "io.vertx.ext.jdbc.spi.impl.C3P0DataSourceProvider").put("max_pool_size", 30);

        jdbcClient = JDBCClient.createShared(vertx, config);
        jdbcClient.getConnection(ar -> {
            SQLConnection connection = ar.result();
            connection.execute(SQL_CREATE_NEWS_TABLE, create -> {
                connection.close();
                if (create.succeeded())
                    future.complete();
                else
                    future.fail(ar.cause());
            });
        });
        return future;
    }

    private Future<Void> startCrawler()
    {
        LOGGER.info("START CRAWLING...");
        LOGGER.info("=======================");
        Future<Void> future = Future.future();
        NewsCrawler nc = new TabnkaNewsCrawler(jdbcClient, webClient);
        vertx.setPeriodic(60000, nc::handle);
        return future;
    }

    public static class TabnkaNewsCrawler implements NewsCrawler
    {
        JDBCClient jdbcClient;
        WebClient webClient;

        public TabnkaNewsCrawler(JDBCClient jdbcClient, WebClient webClient)
        {
            this.jdbcClient = jdbcClient;
            this.webClient = webClient;
        }

        public void handle(Long delay)
        {
            // http://www.tabnak.ir/fa
            webClient.get(80, "tabnak.ir", "/fa").as(BodyCodec.string()).send(ar -> {
                if (ar.succeeded())
                {
                    HttpResponse<String> response = ar.result();
                    String html = response.body();
                    //parse html to find latest news
                    //this is ugly but what can you do
                    Document doc = Jsoup.parse(html);
                    Elements atag = doc.select("div.linear_news h3.Htag a.title5");
                    String attr = atag.attr("href");
                    LOGGER.info("Received response with status code : " + response.statusCode());
                    StringTokenizer tokenizer = new StringTokenizer(attr, "/");
                    if (tokenizer.countTokens() == 4)
                    {
                        String lang = tokenizer.nextToken();
                        String keywords = tokenizer.nextToken();
                        String newsId = tokenizer.nextToken();
                        String address = tokenizer.nextToken();
                        String resourcePath = String.format("%s/%s/%s/%s", lang, keywords, newsId, encodeQuietly(address));
                        String uri = String.format("http://www.tabnak.ir/%s", resourcePath);
                        InputStream is = new JdkHttpConnection().sendGetRequest(uri);
                        LOGGER.info("SEND REQUEST TO " + uri);
                        NewsItem ni = new NewsItem(newsId, lang, inputStreamToString(is), keywords);
                        saveNews(ni);
                    }
                }
            });
        }

        private void saveNews(NewsItem ni)
        {
            jdbcClient.getConnection(h -> {
                SQLConnection connection = h.result();
                StringBuffer queryString = new StringBuffer();
                queryString.append("INSERT INTO NEWS(NEWS_ID, LANG, KEYWORDS, CONTENT, RAW_CONTENT, METADATA) VALUES(?,?,?,?,?,?)");
                JsonArray params = new JsonArray();
                params.add(ni.getNewsId());
                params.add(ni.getLang());
                params.add(ni.getKeywords());
                params.add(ni.getContent());
                params.add(ni.getRawContent());
                params.add(ni.getMetadata());

                connection.updateWithParams(queryString.toString(), params, query -> {
                    if (query.failed())
                    {
                        LOGGER.info("INSERT FAAILED : " + query.cause());
                    }
                    else
                    {
                        LOGGER.info(String.format("INSERT NEWS WITH ID %s TO TABLES", ni.getNewsId()));
                    }
                });
                connection.close();

            });
        }

    }

    public interface NewsCrawler
    {
        public void handle(Long h);
    }

    public static void main(String[] args)
    {
        Runner.runExample(VncVerticle.class);
    }

    public static String inputStreamToString(InputStream is)
    {
        try
        {
            return IOUtils.toString(is);
        }
        catch (IOException e)
        {
            return null;
        }
    }

    public static String encodeQuietly(String address)
    {
        try
        {
            return URLEncoder.encode(address, "UTF-8");
        }
        catch (Exception e)
        {
            return null;
        }
    }

    // Immutable
    public static class NewsItem implements Serializable
    {
        private String newsId;
        private String lang;
        private String content;
        private String keywords;

        public NewsItem(String newsId, String lang, String content, String keywords)
        {
            this.newsId = newsId;
            this.lang = lang;
            this.content = content;
            this.keywords = keywords;
        }

        public String getNewsId()
        {
            return newsId;
        }

        public String getLang()
        {
            return lang;
        }

        public String getContent()
        {
            return content;
        }

        public String getKeywords()
        {
            return keywords;
        }

        private String getRawContent()
        {
            if (content == null)
                return null;
            BodyContentHandler handler = new BodyContentHandler();
            Metadata metadata = new Metadata();
            ParseContext pcontext = new ParseContext();
            HtmlParser htmlparser = new HtmlParser();
            try
            {
                htmlparser.parse(new ByteArrayInputStream(content.getBytes("UTF-8")), handler, metadata, pcontext);
                return handler.toString();
            }
            catch (IOException | SAXException | TikaException e)
            {
                LOGGER.info("Uable to parse html ");
                return null;
            }
        }

        private String getMetadata()
        {
            if (content == null)
                return null;
            BodyContentHandler handler = new BodyContentHandler();
            Metadata metadata = new Metadata();
            ParseContext pcontext = new ParseContext();
            HtmlParser htmlparser = new HtmlParser();
            try
            {
                htmlparser.parse(new ByteArrayInputStream(content.getBytes("UTF-8")), handler, metadata, pcontext);
                return metadata.toString();
            }
            catch (IOException | SAXException | TikaException e)
            {
                LOGGER.info("Unable to get metadata");
                return null;
            }
        }

    }

}
