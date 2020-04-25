package parser;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import enums.Parse;
import enums.Source;
import model.CovidMeta;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ParserThread implements Runnable {


    private static final String INPUT_ROOT = "data";
    private static final String OUTPUT_ROOT = "processed";

    private String name;
    private Source source;
    private Parse parse;

    public ParserThread() {

    }

    public ParserThread(String name, Source source, Parse parse) {
        this.name = name;
        this.source = source;
        this.parse = parse;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public Parse getParse() {
        return parse;
    }

    public void setParse(Parse parse) {
        this.parse = parse;
    }

    public void parseJsonFile() {
        if (name == null || source == null || parse == null)
            throw new IllegalArgumentException("Missing name, source or parse.");
        try {
            parseJsonFile(name, source, parse);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * This function parse a json file and output results according to source and parse type
     *
     * @param name   name of json file
     * @param source the source of the file(biorxv/comm_use/...)
     * @param parse  parse type of the file(pdf/xml/none)
     * @throws IOException
     */
    public void parseJsonFile(String name, Source source, Parse parse) throws IOException {
//        long start = System.currentTimeMillis();
        String suffix = getPathSuffix(name, source, parse);

        CovidMeta covidMeta = new CovidMeta();

        File jsonFilePath = new File(INPUT_ROOT + File.separator + suffix);

        String content = FileUtils.readFileToString(jsonFilePath, StandardCharsets.UTF_8);
        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson(content, JsonObject.class);

        String paper_id = jsonObject.get("paper_id").getAsString();
        covidMeta.setPaperId(paper_id);

        JsonObject metadata = jsonObject.get("metadata").getAsJsonObject();
        String title = metadata.get("title").getAsString();
        covidMeta.setTitle(title);

        JsonArray authorsArray = metadata.get("authors").getAsJsonArray();
        List<String> authors = new ArrayList<>();
        for (int i = 0; i < authorsArray.size(); i++) {
            JsonObject author = authorsArray.get(i).getAsJsonObject();
            String authorName = parseAuthorName(author);
            authors.add(authorName);
        }
        covidMeta.setAuthors(authors);

        if (parse == Parse.PDF) { // PMC json file has no abstract field
            JsonArray abstractArray = jsonObject.get("abstract").getAsJsonArray();
            List<String> textAbstract = new ArrayList<>();
            for (int i = 0; i < abstractArray.size(); i++) {
                JsonObject _abstract = abstractArray.get(i).getAsJsonObject();
                String text = _abstract.get("text").getAsString();
                textAbstract.add(text);
            }
            covidMeta.setTextAbstract(textAbstract);
        }

        JsonArray bodyTextArray = jsonObject.get("body_text").getAsJsonArray();
        List<String> bodyText = new ArrayList<>();
        for (int i = 0; i < bodyTextArray.size(); i++) {
            JsonObject bodyTextObj = bodyTextArray.get(i).getAsJsonObject();
            String paragraph = bodyTextObj.get("text").getAsString();
            bodyText.add(paragraph);
        }
        covidMeta.setBodyText(bodyText);
        FileWriter writer = new FileWriter(OUTPUT_ROOT + File.separator + suffix);

        gson.toJson(covidMeta, writer);
        writer.flush();
        writer.close();
//        long end = System.currentTimeMillis();
//        System.out.println(end - start);
    }

    private String parseAuthorName(JsonObject author) {
        if (author == null || author.isJsonNull())
            return null;
        String first = author.get("first").getAsString();
        JsonArray middleArray = author.get("middle").getAsJsonArray();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < middleArray.size(); i++) {
            sb.append(middleArray.get(i).getAsString() + " ");
        }
        String middle = sb.toString();
        String last = author.get("last").getAsString();
        String suffix = author.get("suffix").getAsString();
        String name = first + " " + middle + " " + last + " " + suffix;
        name = name.replaceAll("\\s+", " ").trim();
        return name;
    }

    private String getPathSuffix(String name, Source source, Parse parse) {
        return source.getPath() + File.separator + parse.getPath() + File.separator + name;
    }

    @Override
    public void run() {
        parseJsonFile();
    }
}
