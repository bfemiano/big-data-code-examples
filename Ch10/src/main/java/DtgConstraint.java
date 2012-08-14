package examples.accumulo;

import org.apache.accumulo.core.constraints.Constraint;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class DtgConstraint implements Constraint {


    private static final short DATE_IN_FUTURE = 1;
    private static final short MALFORMED_DATE = 2;
    private static final byte[] dtgBytes = "dtg".getBytes();
    private static final DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    public String getViolationDescription(short violationCode) {
        if(violationCode == DATE_IN_FUTURE) {
            return "Date cannot be in future";
        } else if(violationCode == MALFORMED_DATE) {
            return "Date does not match simple date format yyyy-MM-dd";
        }
        return null;
    }

    @Override
    public List<Short> check(Environment env, Mutation mutation) {
        List<Short> violations = null;
        try {
            for(ColumnUpdate update : mutation.getUpdates()) {
                if(isDtg(update)) {
                    long dtgTime = dateFormatter.parse(new String(update.getValue())).getTime();
                    long currentMillis = System.currentTimeMillis();
                    if(currentMillis < dtgTime) {
                        violations = checkAndAdd(violations, DATE_IN_FUTURE);
                    }
                }
            }
        } catch (ParseException e) {
            violations = checkAndAdd(violations, MALFORMED_DATE);
        }
        return violations;
    }

    private boolean isDtg(ColumnUpdate update) {
        byte[] qual = update.getColumnQualifier();
        if(qual.length != dtgBytes.length)
            return false;
        for (int i = 0; i < qual.length; i++) {
            if(!(qual[i] == dtgBytes[i])){
               return false;
            }
        }
        return true;
    }

    private List<Short> checkAndAdd(List<Short> violations, short violationCode) {
        if(violations == null)
            violations = new ArrayList<Short>();
        violations.add(violationCode);
        return violations;
    }
}

