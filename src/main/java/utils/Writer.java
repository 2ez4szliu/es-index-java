package utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import enums.Parse;
import enums.Source;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Writer {

    private static final String DATA_ROOT = "data";
    private static final String OUTPUT_PATH = "processed";

    /**
     * Add a new field <key, value> to an existing json object represented by a file
     *
     * @param name   name of the file
     * @param source source of the file(biorxv/comm_use/...)
     * @param parse  parse type(pdf/xml/none)
     * @param key    the key
     * @param value  the value
     * @throws IOException
     */
    public static void writeFieldToJsonFile(String name, Source source, Parse parse, String key, Object value) throws IOException {
        String suffix = source.getPath() + File.separator
                + parse.getPath() + File.separator
                + name;
        File jsonFile = new File(OUTPUT_PATH + File.separator + suffix);

        String content = FileUtils.readFileToString(jsonFile, StandardCharsets.UTF_8);
        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson(content, JsonObject.class);

        jsonObject.addProperty(key, value.toString());

        FileWriter writer = new FileWriter(OUTPUT_PATH + File.separator + suffix);
        gson.toJson(jsonObject, writer);
        writer.flush();
        writer.close();
    }

    public static void main(String[] args) {
        try {
            writeFieldToJsonFile("0a2a28cb82e7a03af0a9fad4fd4c68c9fdac2477.json",
                    Source.BIORXIV,
                    Parse.PDF,
                    "publishTime", "2003/1/13");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
