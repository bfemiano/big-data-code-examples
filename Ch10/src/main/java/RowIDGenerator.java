package mil.rebel.taint.accumulo;

import javax.security.auth.login.Configuration;
import java.io.IOException;

public interface RowIDGenerator {

    public String getRowID(String[] parameters)
            throws IllegalArgumentException;
}
