package parser;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import enums.Source;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MetadataCSVParser {

    private static String CSV_FILE_PATH = "data/metadata.csv";
    private static int THREAD_POOL_SIZE = 10;
    private static Map<String, Source> SOURCE_MAP = new HashMap<>();

    static {
        SOURCE_MAP.put("custom_license", Source.CUSTOM_LICENSE);
        SOURCE_MAP.put("comm_use_subset", Source.COMM_USE);
        SOURCE_MAP.put("noncomm_use_subset", Source.NON_COMM_USE);
        SOURCE_MAP.put("biorxiv_medrxiv", Source.BIORXIV);
    }

    private CsvSchema schema;

    public MetadataCSVParser() {
        schema = CsvSchema.builder()
                .addColumn("cord_uid")
                .addColumn("sha")
                .addColumn("source_x")
                .addColumn("title")
                .addColumn("doi")
                .addColumn("pmcid")
                .addColumn("pubmed_id")
                .addColumn("license")
                .addColumn("abstract")
                .addColumn("publish_time")
                .addColumn("authors")
                .addColumn("journal")
                .addColumn("Microsoft Academic Paper ID")
                .addColumn("WHO_Covidence")
                .addColumn("has_pdf_parse")
                .addColumn("has_pmc_xml_parse")
                .addColumn("full_text_file")
                .addColumn("url")
                .build();
    }

    public void parseCSV() throws IOException {

        File csvFile = new File(CSV_FILE_PATH);
        CsvMapper csvMapper = new CsvMapper();
        MappingIterator<Map<String, String>> it = csvMapper.readerFor(Map.class)
                .with(schema)
                .readValues(csvFile);
        it.nextValue(); //skip first row of property names
        int id = 0;
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        while (it.hasNext()) {
            Map<String, String> next = it.nextValue();
            MetadataCSVParserThread thread = new MetadataCSVParserThread();
            thread.setId(id);
            thread.setRow(next);
            pool.execute(thread);
            id++;
        }
        pool.shutdown();
    }

    public static void main(String[] args) {
        MetadataCSVParser parser = new MetadataCSVParser();
        try {
            parser.parseCSV();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
