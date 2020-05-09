import com.google.gson.Gson;
import model.CovidMeta;
import org.apache.commons.io.FileUtils;
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
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


public class IndexJsonFile {


    String indexName, indexTypeName;
    Client client = null;

    public static final String ID = "id";
    public static final String PAPER_ID = "paperId";
    public static final String SHA = "sha";
    public static final String PMCID = "pmcid";
    public static final String TITLE = "title";
    public static final String ABSTRACT = "textAbstract";
    public static final String BODY_TEXT = "bodyText";
    public static final String AUTHORS = "authors";
    public static final String PUBLISH_TIME = "publishTime";
    public static final String URL = "url";

    public static final String FINAL_INDEX_NAME = "covid_index";

    public static final String INDEX_DATA_ROOT = "index-data";
    public static final String JSON_SUFFIX = ".json";

    public static final int BULK_REQUEST_SIZE = 1000;


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
        indexName = FINAL_INDEX_NAME;
        indexTypeName = "_doc";
    }

    /*
    Method used to init Elastic Search Transport client,
    Return true if it is successfully initialized otherwise false
     */
    public boolean initEStransportClinet() {
        try {
            //connect to elastic cloud
            Settings esSettings = Settings.builder()
                    .put("cluster.name", "cosi132a")
                    .build();
            client = new PreBuiltTransportClient(esSettings)
                    .addTransportAddress(
                            new TransportAddress(InetAddress.getByName("localhost"), 9300));

            // Analysis setting
            // @formatter:off
            XContentBuilder analysisBuilder = XContentFactory.jsonBuilder()
                    .startObject()
                        .startObject("analysis")
                            .startObject("filter")
                                .startObject("filter_stemmer")
                                    .field("type", "porter_stem")
                                    .field("language", "English")
                                .endObject()
                            .endObject()
                            .startObject("analyzer")
                                .startObject("my-analyzer")
                                    .field("tokenizer", "standard")
                                    .array("filter", "stop", "lowercase", "filter_stemmer")
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
                            .startObject("id")
                                .field("type", "text")
                            .endObject()
                            .startObject("paperId")
                                .field("type", "text")
                            .endObject()
                            .startObject("title")
                                .field("type", "text")
                                .field("analyzer", "my-analyzer")
                            .endObject()
                            .startObject("sha")
                                .field("type", "text")
                            .endObject()
                            .startObject("pmcid")
                                .field("type", "text")
                            .endObject()
                            .startObject("textAbstract")
                                .field("type", "text")
                                .field("analyzer", "my-analyzer")
                            .endObject()
                            .startObject("authors")
                                .field("type", "text")
                                .field("analyzer", "my-analyzer")
                            .endObject()
                            .startObject("textBody")
                                .field("type", "text")
                                .field("analyzer", "my-analyzer")
                            .endObject()
                            .startObject("publishTime")
                                .field("type", "date")
                                .field("format", "yyyy-MM-dd")
                            .endObject()
                            .startObject("url")
                                .field("type", "text")
                            .endObject()
                        .endObject()
                    .endObject();
            // @formatter:on
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

        int count = 0, noOfBatch = 1;
        String[] fileNames = new File(INDEX_DATA_ROOT).list();

        int numberOfRecords = 0;
        for (int i = 0; i < fileNames.length; i++) {
            String name = i + JSON_SUFFIX;
            CovidMeta document = getJsonObj(name);
            try {
                XContentBuilder xContentBuilder = jsonBuilder()
                        .startObject()
                        .field(ID, count + "")
                        .field(PAPER_ID, document.getPaperId())
                        .field(SHA, document.getSha())
                        .field(PMCID, document.getPmcid())
                        .field(TITLE, document.getTitle())
                        .field(ABSTRACT, document.getTextAbstract())
                        .field(AUTHORS, document.getAuthors())
                        .field(BODY_TEXT, document.getBodyText())
                        .field(PUBLISH_TIME, DateConverter.parseDateStr(document.getPublishTime()))
                        .field(URL, document.getUrl())
                        .endObject();

                bulkRequest.add(client.prepareIndex(indexName, indexTypeName, String.valueOf(numberOfRecords))
                        .setSource(xContentBuilder));

                if (count == BULK_REQUEST_SIZE) {
                    addDocumentToESCluser(bulkRequest, noOfBatch, count);
                    bulkRequest = client.prepareBulk();
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

    private CovidMeta getJsonObj(String jsonFileName) throws IOException {
        String jsonFilePath = INDEX_DATA_ROOT + File.separator + jsonFileName;
        File jsonFile = new File(jsonFilePath);
        String content = FileUtils.readFileToString(jsonFile, StandardCharsets.UTF_8);
        Gson gson = new Gson();
        return gson.fromJson(content, CovidMeta.class);
    }

    public void closeTransportClient() {
        if (client != null) {
            client.close();
        }
    }

}