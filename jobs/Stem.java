import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;
import java.io.IOException;

public class STEM extends EvalFunc<String> {
  public String exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0)
      return null;
    try {
      String str = (String)input.get(0);
      Porter stem = new Porter();
      return stem.stripAffixes(str);
    } catch(Exception e) {
      System.err.println("Failed to process input; error - " + e.getMessage());
      return null;
    }
  }
}
