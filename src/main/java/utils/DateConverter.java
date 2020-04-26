package utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DateConverter {

    public static Map<String, Integer> monthAbbreviationToNumber = new HashMap<>();
    public static SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
    public static String DEFAULT_DATE = "2019-01-01";

    static {
        monthAbbreviationToNumber.put("Jan", 0);
        monthAbbreviationToNumber.put("Feb", 1);
        monthAbbreviationToNumber.put("Mar", 2);
        monthAbbreviationToNumber.put("Apr", 3);
        monthAbbreviationToNumber.put("May", 4);
        monthAbbreviationToNumber.put("Jun", 5);
        monthAbbreviationToNumber.put("Jul", 6);
        monthAbbreviationToNumber.put("Aug", 7);
        monthAbbreviationToNumber.put("Sep", 8);
        monthAbbreviationToNumber.put("Oct", 9);
        monthAbbreviationToNumber.put("Nov", 10);
        monthAbbreviationToNumber.put("Dec", 11);

    }

    public static String parseDateStr(String input) {
        if (input == null || input.isEmpty())
            return DEFAULT_DATE;
        String[] ymd = input.split("-");
        if (ymd.length == 3)
            return input;
        else if (ymd.length == 2) {
            return input + "-01";
        } else {
            return input + "-01-01";
        }

    }

    public static long dateToLong(String date) {
        String[] ymd = date.split(" ");
        if (ymd.length != 3)
            return 0;
        int y = Integer.parseInt(ymd[0]);
        int m = monthAbbreviationToNumber.get(ymd[1]);
        int d = Integer.parseInt(ymd[2]);

        //Feb 31
        if (m == 1 && d > 29)
            return 0;
        //June31
        if (m == 5 && d > 30)
            return 0;
        return new Date(y, m, d).getTime();
    }

    public static Date longToDate(long time) {
        Date date = new Date(time);
        date.setYear(date.getYear() - 1900);
        return date;
    }

    public static String longToFormattedDate(long time) {
        Date date = longToDate(time);
        return sdf.format(date);
    }

    public static void main(String[] args) {
        String date = "2006-08";
        System.out.println(parseDateStr(date));
    }
}
