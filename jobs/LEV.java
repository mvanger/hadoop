import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;
import java.io.IOException;

public class LEV extends EvalFunc<Integer> {
  public Integer exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0)
      return null;
    try {
      String str_one = (String)input.get(0);
      String str_two = (String)input.get(1);
      Levenshtein lev = new Levenshtein();
      return lev.getLevenshteinDistance(str_one, str_two);
    } catch(Exception e) {
      System.err.println("Failed to process input; error - " + e.getMessage());
      return null;
    }
  }
}
