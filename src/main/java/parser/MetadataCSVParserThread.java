package parser;

import com.google.gson.Gson;
import enums.Parse;
import enums.Source;
import model.CovidMeta;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class MetadataCSVParserThread implements Runnable {

    private static String CSV_FILE_PATH = "data/metadata.csv";
    private static String OUTPUT_PATH = "index-data";
    private static String JSON_DATA_ROOT = "processed";
    private static String PMC_SUFFIX = ".xml.json";
    private static String PDF_SUFFIX = ".json";
    private static Map<String, Source> SOURCE_MAP = new HashMap<>();

    static {
        SOURCE_MAP.put("custom_license", Source.CUSTOM_LICENSE);
        SOURCE_MAP.put("comm_use_subset", Source.COMM_USE);
        SOURCE_MAP.put("noncomm_use_subset", Source.NON_COMM_USE);
        SOURCE_MAP.put("biorxiv_medrxiv", Source.BIORXIV);
    }

    private Map<String, String> row;
    private int id;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Map<String, String> getRow() {
        return row;
    }

    public void setRow(Map<String, String> row) {
        this.row = row;
    }

    /**
     * Update the pdf parse data using a row in the csv file
     * @param row a row in csv file
     * @param covidMeta pdf-parsed data, representing same documents as row
     * @param id the id of the doc(sha)
     * @throws IOException
     */
    private void updatePDFParse(Map<String, String> row, CovidMeta covidMeta, int id) throws IOException {
        covidMeta.setSha(row.get("sha"));
        covidMeta.setPmcid(row.get("pmcid"));
        covidMeta.setUrl(row.get("url"));
        covidMeta.setPublishTime(row.get("publish_time"));

        Gson gson = new Gson();
        FileWriter writer = new FileWriter(OUTPUT_PATH + File.separator + id + ".json");
        gson.toJson(covidMeta, writer);
        writer.flush();
        writer.close();
    }

    /**
     *
     * Update the pmc parse data using a row in the csv file
     * @param row a row in csv file
     * @param covidMeta pmc-parsed data, representing same documents as row
     * @param id the id of the doc(pmcid)
     * @throws IOException
     */

    private void updatePMCParse(Map<String, String> row, CovidMeta covidMeta, int id) throws IOException {
        covidMeta.setSha(row.get("sha"));
        covidMeta.setPmcid(row.get("pmcid"));
        covidMeta.setUrl(row.get("url"));
        covidMeta.setPublishTime(row.get("publish_time"));
        //PMC file at first has no abstract field
        String _abstract = row.get("abstract");
        List<String> textAbstract = Arrays.asList(_abstract.split("\n"));
        covidMeta.setTextAbstract(textAbstract);

        Gson gson = new Gson();
        FileWriter writer = new FileWriter(OUTPUT_PATH + File.separator + id + ".json");
        gson.toJson(covidMeta, writer);
        writer.flush();
        writer.close();

    }

    /**
     * Get a CovidMeta Object from a given source and parse format
     * @param id id of the object(pmcid or sha)
     * @param source source
     * @param parse pdf/pmc
     * @return a covidMeta object of the document
     * @throws IOException
     */
    private CovidMeta getJsonObjFromSource(String id, Source source, Parse parse) throws IOException {
        String jsonFilePath = JSON_DATA_ROOT + File.separator +
                source.getPath() + File.separator
                + parse.getPath() + File.separator
                + id;
        File jsonFile = new File(jsonFilePath);
        String content = FileUtils.readFileToString(jsonFile, StandardCharsets.UTF_8);
        Gson gson = new Gson();
        return gson.fromJson(content, CovidMeta.class);
    }

    /**
     * Create a json object from a row of csv file, and write it to a json file for indexing
     *
     * @param row a row of csv file, Class: Map<String, String>
     */
    private void createJsonObjFromRow(Map<String, String> row, int id) throws IOException {
        CovidMeta covidMeta = new CovidMeta();
        covidMeta.setSha(row.get("sha"));
        covidMeta.setTitle(row.get("title"));
        covidMeta.setPmcid(row.get("pmcid"));
        covidMeta.setPublishTime(row.get("publish_time"));
        covidMeta.setUrl(row.get("url"));

        String _abstract = row.get("abstract");
        List<String> textAbstract = Arrays.asList(_abstract.split("\n"));
        covidMeta.setTextAbstract(textAbstract);

        String _authors = row.get("authors");
        List<String> authors = new ArrayList<>();
        for (String _author : _authors.split(";")) {
            String[] firstLast = _author.split(",");
            String name = firstLast.length == 1 ? firstLast[0] : firstLast[1] + " " + firstLast[0];
            name = name.replaceAll("\\s+", " ").trim();
            authors.add(name);
        }
        covidMeta.setAuthors(authors);

        Gson gson = new Gson();
        FileWriter writer = new FileWriter(OUTPUT_PATH + File.separator + id + ".json");
        gson.toJson(covidMeta, writer);
        writer.flush();
        writer.close();
    }

    @Override
    public void run() {
        boolean hasPdfParse = row.get("has_pdf_parse").toLowerCase().equals("true");
        boolean hasPmcParse = row.get("has_pmc_xml_parse").toLowerCase().equals("true");
        try {
            if (!hasPdfParse && !hasPmcParse) {
                //In this case,we need to create a new CovidMeta object
                createJsonObjFromRow(row, id);
            } else if (!hasPdfParse && hasPmcParse) {
                //Find pmc file using pmcid as key, update field, write to json
                String KEY = row.get("pmcid") + PMC_SUFFIX;
                Source source = SOURCE_MAP.get(row.get("full_text_file"));
                CovidMeta covidMeta = getJsonObjFromSource(KEY, source, Parse.PMC);
                updatePMCParse(row, covidMeta, id);
            } else if (hasPdfParse && !hasPmcParse) {
                //Find pdf parse file using sha as key update field, write to json
                String[] arr = row.get("sha").split(";");
                Arrays.sort(arr);
                String KEY = arr[0].trim() + PDF_SUFFIX;
                Source source = SOURCE_MAP.get(row.get("full_text_file"));
                CovidMeta covidMeta = getJsonObjFromSource(KEY, source, Parse.PDF);
                updatePDFParse(row, covidMeta, id);
            } else {
                // In case file has both PDF & PMC parse
                // Use PMC file, since the sha field could contain multiple sha number
                String KEY = row.get("pmcid") + PMC_SUFFIX;
                Source source = SOURCE_MAP.get(row.get("full_text_file"));
                String pmcFilePath = JSON_DATA_ROOT + File.separator +
                        source.getPath() + File.separator
                        + Parse.PMC.getPath() + File.separator
                        + KEY;
                if (new File(pmcFilePath).exists()) {
                    CovidMeta covidMeta = getJsonObjFromSource(KEY, source, Parse.PMC);
                    updatePMCParse(row, covidMeta, id);
                } else {
                    KEY = row.get("sha") + PDF_SUFFIX;
                    CovidMeta covidMeta = getJsonObjFromSource(KEY, source, Parse.PDF);
                    updatePDFParse(row, covidMeta, id);
                }
            }
        } catch (
                IOException e) {
            e.printStackTrace();
        }
    }
}
