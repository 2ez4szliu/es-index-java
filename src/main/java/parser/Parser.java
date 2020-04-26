package parser;

import enums.Parse;
import enums.Source;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Parser {

    private static final int THREAD_POOL_SIZE = 10;
    private static final String INPUT_ROOT = "data";
    private static final String OUTPUT_ROOT = "processed";

    public Parser() {
    }

    /**
     * Main program that parses all json files in original data folders
     * only keep the field that we need to build index
     * generate new json files
     *
     * @param args
     */
    public static void main(String[] args) {
        if (!new File(OUTPUT_ROOT).exists()) {
            Parser.makeDirectories();
        }

        // Run the following code will parse folders one by one
        //comment/uncomment source/parse pair

        //1
//        Source source = Source.BIORXIV;
//        Parse parse = Parse.PDF;

        //2
//        Source source = Source.COMM_USE;
//        Parse parse = Parse.PDF;
        //3
//        Source source = Source.COMM_USE;
//        Parse parse = Parse.PMC;

        //4
//        Source source = Source.CUSTOM_LICENSE;
//        Parse parse = Parse.PDF;

        //5
//        Source source = Source.CUSTOM_LICENSE;
//        Parse parse = Parse.PMC;

        //6
//        Source source = Source.NON_COMM_USE;
//        Parse parse = Parse.PDF;

        //7
//        Source source = Source.NON_COMM_USE;
//        Parse parse = Parse.PMC;

//        Parser parser = new Parser();
//        try {
//            parser.parseAllFilesInFolder(source, parse);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        // Code to parse all files together
        try {
            for (Source source : Source.values()) {
                for (Parse parse : Parse.values()) {
                    if (source == Source.BIORXIV && parse == Parse.PMC)
                        continue;
                    Parser parser = new Parser();
                    parser.parseAllFilesInFolder(source, parse);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<String> getAllFileNames(String root, Source source, Parse parse) {
        String suffix = source.getPath() + File.separator + parse.getPath();
        File folder = new File(root + File.separator + suffix);
        File[] files = folder.listFiles();
        List<String> result = new ArrayList<>();
        for (File file : files) {
            result.add(file.getName());
        }
        return result;
    }

    public void parseAllFilesInFolder(Source source, Parse parse) throws IOException {
        //Make use of thread pool to manage thread's lifecycle
        List<String> names = getAllFileNames(INPUT_ROOT, source, parse);
        int numberOfNames = names.size();
        int count = numberOfNames;
        int id = 0;
        ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        while (count > 0 && id < numberOfNames) {
            String name = names.get(id);
            count--;
            id++;
            ParserThread parserThread = new ParserThread(name, source, parse);
            pool.execute(parserThread);
        }
        pool.shutdown();

    }

    public static void makeDirectories() {
        for (Source source : Source.values())
            for (Parse parse : Parse.values()) {
                //BIOXRIV has no pmc parse
                if (source == Source.BIORXIV && parse == Parse.PMC)
                    continue;
                new File(OUTPUT_ROOT + File.separator +
                        source.getPath() + File.separator +
                        parse.getPath()).mkdirs();
            }
    }

}
