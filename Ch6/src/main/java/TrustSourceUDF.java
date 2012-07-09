import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.lang.String;import java.util.HashSet;
import java.util.Set;


public class TrustSourceUDF extends UDF {

    private static Set<String> untrustworthySources = new HashSet<String>();
    private Text result = new Text();

    static{
        untrustworthySources.add("");
        untrustworthySources.add("\"\"\"http://www.afriquenligne.fr/3-soldiers\"");
        untrustworthySources.add("Africa News Service");
        untrustworthySources.add("Asharq Alawsat");
        untrustworthySources.add("News Agency of Nigeria (NAN)");
        untrustworthySources.add("This Day (Nigeria)");
    }

    @Override
    public Text evaluate(Text source) {

         if (untrustworthySources.contains(source.toString())){
             result.set("no");
         } else {
             result.set("yes");
         }
         return result;
    }
}
