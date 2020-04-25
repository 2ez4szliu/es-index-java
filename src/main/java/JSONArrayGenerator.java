import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

public class JSONArrayGenerator {
    private static int NUM_OF_OBJECTS = 8477;
    public static void main(String[] args) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader("/data/covid_comm_use_subset_meta.json"));
            PrintWriter pw = new PrintWriter("/data/covid_metadata.json");
            pw.write("[" + "\n");
            int count = 0;
            String line = reader.readLine();
            while (line != null) {
                count++;
                int i1 = line.indexOf("\"authors\"");
                int i2 = line.indexOf("\"publish_time\"");
                String authors = line.substring(i1 + 12, i2 - 3);
                String[] authorsArray = authors.split(";");
                for (int i = 0; i < authorsArray.length; i++) {
                    authorsArray[i] = authorsArray[i].trim();
                }
                StringBuilder sb = new StringBuilder();
                sb.append("[");
                for (String author : authorsArray) {
                    String[] firstLast = author.split(",");
//                    System.out.println(Arrays.toString(firstLast));
//                    String name = firstLast[0] + " " + firstLast[1];
                    String name = firstLast.length > 1 ? firstLast[0]  + firstLast[1] : firstLast[0];
                    sb.append("\"" + name.trim() + "\",");
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.append("]");
                String newLine = line.substring(0, i1 + 11) + sb.toString() + line.substring(i2 - 2);
                newLine = newLine.replaceAll("publish_time", "publishTime");
                newLine = newLine.replaceAll("abstract", "textAbstract");
                pw.write(newLine);
                if (count < NUM_OF_OBJECTS)
                    pw.write("," + "\n");
                line = reader.readLine();

            }
            pw.write("]");
            pw.close();
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
