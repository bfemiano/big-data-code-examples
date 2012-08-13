package examples.accumulo;

import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ValidatingKeyGenTest {

    private ACLEDRowIDGenerator keyGen;
    private SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    @BeforeClass
    public void setup() {
        keyGen = new ACLEDRowIDGenerator();
    }

    @Test
    public void validZOrder() {
        try {
            String zpoint = keyGen.getZOrderedCurve("33.22", "44.55");  //+90 = 123.22,134.55
            assertEquals(zpoint, "1123342525");

            zpoint = keyGen.getZOrderedCurve("33", "44.55"); //+90 = 123, 134.55
            assertEquals(zpoint, "1123340505");

            zpoint = keyGen.getZOrderedCurve("33.55", "44"); //+90 = 123.55, 134
            assertEquals(zpoint, "1123345050");

            zpoint = keyGen.getZOrderedCurve("33.1234", "44.56"); //+90 = 123.1234, 134.56
            assertEquals(zpoint, "11233415263040");

            zpoint = keyGen.getZOrderedCurve("-90.11", "44.56"); //+90 = 00.11, 134.56
            assertEquals(zpoint, "0103041516");

            zpoint = keyGen.getZOrderedCurve("-85.11", "44.56"); //+90 = 005.11, 134.56
            assertEquals(zpoint, "0103541516");

            zpoint = keyGen.getZOrderedCurve("-79.11", "44.56"); //+90 = 011.11, 134.56
            assertEquals(zpoint, "0113141516");

            zpoint = keyGen.getZOrderedCurve("5", "44.56"); //+90 = 095, 134.56
            assertEquals(zpoint, "0193540506");

        } catch (Exception e) {
            fail("EXCEPTION fail: " + e.getMessage());
        }

    }

    @Test
    public void invalidZOrder() {
        String zpoint = null;
        try {
            zpoint = keyGen.getZOrderedCurve("98.22", "33.44");
            fail("Shoud not parse. Too big an integral value.");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid integral"));
        }

        try {
            zpoint = keyGen.getZOrderedCurve("78.22", "-91.44");
            fail("Shoud not parse. Too big an integral value.");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("invalid integral"));
        }

        try {
            zpoint = keyGen.getZOrderedCurve("332.22.33", "33.44.33.22");
            fail("Shoud not parse. Too many split values.");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Malformed point"));
        }

        try {
            zpoint = keyGen.getZOrderedCurve("33.22a", "33.33");
            fail("Shoud not parse. Contains bad characters.");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("contains non-numeric characters"));
        }

        try {
            zpoint = keyGen.getZOrderedCurve("33.22", "3c.33");
            fail("Shoud not parse. Contains bad characters.");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("contains non-numeric characters"));
        }
    }

    @Test
    public void testValidReverseTime() {
        String dateStr = "2012-05-23";
        long reverse = keyGen.getReverseTime(dateStr);
        try {
            Date date = dateFormatter.parse(dateStr);
            assertEquals(reverse, (Long.MAX_VALUE - date.getTime()));
        } catch (ParseException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testInvalidReverseTime() {
        try {
            long reverse = keyGen.getReverseTime("201a-22-22");
            fail("Should not reverse invalid date for DateFormat");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("could not be parsed to a valid date with the supplied DateFormat"));
        }
    }

    @Test
    public void testFullKey() {
        try {
            String dateStr = "2012-03-13";
            Date date = dateFormatter.parse(dateStr);
            long reverse = Long.MAX_VALUE - date.getTime();
            String key = keyGen.getRowID(new String[]{"33.55", "66.77", dateStr}); //+90 = 123.55, 156.77
            assertEquals(key, "1125365757_" + reverse);
        } catch (ParseException e) {
            fail(e.getMessage());
        } catch (IllegalArgumentException e) {
            fail(e.getMessage());
        }
    }

}
