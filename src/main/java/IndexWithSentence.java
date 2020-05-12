import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class IndexWithSentence {
    String indexName, indexTypeName;
    Client client = null;

    public static final String ID = "id";
    public static final String TEXT = "text";
    public static final String VECTOR = "vector";

    public static final String EMBEDDING_INDEX_NAME = "sentence_embedding_index";

    public static final String INDEX_DATA_ROOT = "sentence_embedding-data";
    public static final String JSON_SUFFIX = ".json.em.josn";

    public static final int DIMENSION = 300;
    public static final int BULK_REQUEST_SIZE = 5000;

    public static void main(String[] args) {
        IndexWithSentence esExample = new IndexWithSentence();
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


    public IndexWithSentence() {
        indexName = EMBEDDING_INDEX_NAME;
        indexTypeName = "_doc";
    }

    /*
    Method used to init Elastic Search Transport client,
    Return true if it is successfully initialized otherwise false
     */
    public boolean initEStransportClinet() {
        try {
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
                            .startObject("text")
                                .field("type", "text")
                                .field("analyzer", "my-analyzer")
                            .endObject()
                            .startObject("vector")
                                .field("type", "dense_vector")
                                .field("dims", DIMENSION)
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
        Gson gson = new Gson();
        for (int i = 0; i < fileNames.length; i++) {
            String name = INDEX_DATA_ROOT + File.separator + i + JSON_SUFFIX;
            File file = new File(name);
            String content = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
            JsonObject jsonObject = gson.fromJson(content, JsonObject.class);
            JsonArray sentences = jsonObject.get(i + "").getAsJsonArray();
            for (int j = 0; j < sentences.size(); j++) {
                JsonObject obj = sentences.get(j).getAsJsonObject();
                String sentence = obj.get("sentence").getAsString();
                double[] vector = gson.fromJson(obj.get("vector"), double[].class);
                try {
                    XContentBuilder xContentBuilder = jsonBuilder()
                            .startObject()
                            .field(ID, i)
                            .field(TEXT, sentence)
                            .field(VECTOR, vector)
                            .endObject();

                    bulkRequest.add(client.prepareIndex(indexName, indexTypeName,
                            String.valueOf(numberOfRecords))
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

    public void closeTransportClient() {
        if (client != null) {
            client.close();
        }
    }


}
