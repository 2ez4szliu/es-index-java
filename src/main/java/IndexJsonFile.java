import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import model.CovidMeta;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import utils.DateConverter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


public class IndexJsonFile {


    String indexName, indexTypeName;
    Client client = null;

    public static final String SHA = "sha";
    public static final String TITLE = "title";
    public static final String ABSTRACT = "textAbstract";
    public static final String AUTHORS = "authors";
    public static final String PUBLISH_TIME = "publishTime";

    public static final String SAMPLE_INDEX_NAME = "covid_sample_index";
    public static final String FULL_INDEX_NAME = "covid_full_index";


    public static void main(String[] args) {
        IndexJsonFile esExample = new IndexJsonFile();
        try {
            esExample.initEStransportClinet(); //init transport client
            long start = System.currentTimeMillis();
            esExample.JsonBulkImport(); //index multiple  document
            long end = System.currentTimeMillis();
            System.out.println("Total time of indexing:");
            System.out.println((end - start) + " ms");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            esExample.closeTransportClient(); //close transport client
        }
    }


    public IndexJsonFile() {
        indexName = SAMPLE_INDEX_NAME;
        indexTypeName = "_doc";
    }

    /*
    Method used to init Elastic Search Transport client,
    Return true if it is successfully initialized otherwise false
     */
    public boolean initEStransportClinet() {
        try {
            // un-command this, if you have multiple node
            Settings esSettings = Settings.builder()
                    .put("cluster.name", "cosi132a")
                    .build();
            client = new PreBuiltTransportClient(esSettings)
                    .addTransportAddress(
                            new TransportAddress(InetAddress.getByName("localhost"), 9300));

            // Analysis setting
            XContentBuilder analysisBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                        .startObject("analysis")
                            .startObject("filter")
                                .startObject("filter_stemmer")
                                    .field("type","porter_stem")
                                    .field("language","English")
                                .endObject()
                            .endObject()
                            .startObject("analyzer")
                                .startObject("my-analyzer")
                                    .field("tokenizer","standard")
                                    .array("filter","stop","lowercase","filter_stemmer")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject();
            client.admin().indices()
                    .prepareCreate(indexName)
                    .setSettings(analysisBuilder).get();

            // Mapping settings
            XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                        .startObject("properties")
                            .startObject("title")
                                .field("type","text")
                                .field("analyzer", "my-analyzer")
                            .endObject()
                            .startObject("sha")
                                .field("type", "text")
                            .endObject()
                            .startObject("textAbstract")
                                .field("type","text")
                                .field("analyzer", "my-analyzer")
                            .endObject()
                            .startObject("authors")
                                .field("type", "text")
                                .field("analyzer", "my-analyzer")
                            .endObject()
                            .startObject("publishTime")
                                .field("type", "long")
                            .endObject()
                        .endObject()
                    .endObject();
            client.admin().indices()
                    .preparePutMapping(indexName)
                    .setType(indexTypeName)
                    .setSource(mappingBuilder)
                    .execute().actionGet();
            return true;
        } catch (Exception ex) {
            //log.error("Exception occurred while getting Client : " + ex, ex);
            ex.printStackTrace();
            return false;
        }
    }


    public void JsonBulkImport() throws IOException {

        BulkRequestBuilder bulkRequest = client.prepareBulk();

        File jsonFilePath = new File("/data/covid_metadata.json");
        int count = 0, noOfBatch = 1;

        //initialize jsonReader class by passing reader
        JsonReader jsonReader = new JsonReader(
                new InputStreamReader(
                        new FileInputStream(jsonFilePath), StandardCharsets.UTF_8));

        Gson gson = new GsonBuilder().create();

        jsonReader.beginArray(); //start of json array
        int numberOfRecords = 1;
        while (jsonReader.hasNext()) { //next json array element
            CovidMeta document = gson.fromJson(jsonReader, CovidMeta.class);
            //do something real
            try {
                XContentBuilder xContentBuilder = jsonBuilder()
                        .startObject()
                            .field(SHA, document.getSha())
                            .field(TITLE, document.getTitle())
                            .field(ABSTRACT, document.getTextAbstract())
                            .field(AUTHORS, document.getAuthors())
                            .field(PUBLISH_TIME, DateConverter.dateToLong(document.getPublishTime()))
                        .endObject();

                bulkRequest.add(client.prepareIndex(indexName, indexTypeName, String.valueOf(numberOfRecords))
                        .setSource(xContentBuilder));

                if (count == 500) {
                    addDocumentToESCluser(bulkRequest, noOfBatch, count);
                    noOfBatch++;
                    count = 0;
                }
            } catch (Exception e) {
                e.printStackTrace();
                //skip records if wrong date in input file
            }
            numberOfRecords++;
            count++;
        }
        jsonReader.endArray();
        if (count != 0) { //add remaining documents to ES
            addDocumentToESCluser(bulkRequest, noOfBatch, count);
        }
        System.out.println("Total Document Indexed : " + numberOfRecords);
    }

    public void addDocumentToESCluser(BulkRequestBuilder bulkRequest, int noOfBatch, int count) {

        if (count == 0) {
            //org.elasticsearch.action.ActionRequestValidationException: Validation Failed: 1: no requests added;
            return;
        }
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            System.out.println("Bulk Indexing failed for Batch : " + noOfBatch);

            // process failures by iterating through each bulk response item
            int numberOfDocFailed = 0;
            Iterator<BulkItemResponse> iterator = bulkResponse.iterator();
            while (iterator.hasNext()) {
                BulkItemResponse response = iterator.next();
                if (response.isFailed()) {
                    //System.out.println("Failed Id : "+response.getId());
                    numberOfDocFailed++;
                }
            }
            System.out.println("Out of " + count + " documents, " + numberOfDocFailed + " documents failed");
            System.out.println(bulkResponse.buildFailureMessage());
        } else {
            System.out.println("Bulk Indexing Completed for batch : " + noOfBatch);
        }
    }

    public void closeTransportClient() {
        if (client != null) {
            client.close();
        }
    }

}