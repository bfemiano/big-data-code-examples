package examples.accumulo;

import java.lang.IllegalArgumentException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

public class ACLEDRowIDGenerator implements RowIDGenerator {

    private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final Pattern decimalPattern = Pattern.compile("[.]");

    @Override
    public String getRowID(String[] parameters)
            throws IllegalArgumentException{
        if(parameters.length != 3)
            throw new IllegalArgumentException("Required: {lat, lon, dtg}");
        StringBuilder builder = new StringBuilder();
        builder.append(getZOrderedCurve(parameters[0], parameters[1]));
        builder.append("_");
        builder.append(getReverseTime(parameters[2]));
        return builder.toString();
    }

    public String getZOrderedCurve(String lat, String lon)
            throws IllegalArgumentException {
        StringBuilder builder = new StringBuilder();
        lat = cleanAndValidatePoint(lat);
        lon = cleanAndValidatePoint(lon);
        int ceiling = Math.max(lat.length(), lon.length());
        for (int i = 0; i < ceiling; i++) {
            if(lat.length() <= i) {
                builder.append("0");
            } else {
                builder.append(lat.charAt(i));
            }
            if(lon.length() <= i) {
                builder.append("0");
            } else {
                builder.append(lon.charAt(i));
            }
        }
        return builder.toString();
    }

    private String cleanAndValidatePoint(String point)
            throws IllegalArgumentException {

        String[] pointPieces = decimalPattern.split(point);
        if(pointPieces.length > 2) {
            throw new IllegalArgumentException("Malformed point: " + point);
        }
        String integralStr = null;
        int integral = 0;
        try {
            integral = Integer.parseInt(pointPieces[0]) + 90; //offset any negative integral portion
            if(integral > 180 | integral < 0) {
                throw new IllegalArgumentException("invalid integral: " + integral + " for point: " + point);
            }
            integralStr = "" + integral;
            if(pointPieces.length > 1)
                integralStr += Integer.parseInt(pointPieces[1]);
            if(integral < 10)
                integralStr = "00" + integralStr;
            else if (integral >= 10 && integral < 100)
                integralStr = "0" + integralStr;
            return  integralStr;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("point: " + point + " contains non-numeric characters");
        }
    }

    public long getReverseTime(String dateTime)
            throws IllegalArgumentException {
        Date date = null;
        try {
            date = dateFormat.parse(dateTime);
        } catch (ParseException e) {
            throw new IllegalArgumentException(dateTime + " could not be parsed to a " +
                    "valid date with the supplied DateFormat " + dateFormat.toString());
        }
        return Long.MAX_VALUE - date.getTime();
    }
}
