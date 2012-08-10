package mil.rebel.taint.accumulo;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * User: bfemiano
 * Date: 8/9/12
 * Time: 8:44 PM
 */
public class CellValues implements Writable {

        String[] cellValues;

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(cellValues.length);
            for(String cell : cellValues)
                out.writeUTF(cell);

        }

        @Override
        public void readFields(DataInput in) throws IOException {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                cellValues[i] = in.readUTF();

            }
        }

        public CellValues() {}

        public String[] getCellValues() {
            return cellValues;
        }

        public void setCellValues(String[] cellValues) {
            this.cellValues = cellValues;
        }
    }
