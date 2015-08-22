import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
 
import java.io.IOException;
import java.util.Date;


public class JsonDateSerializer extends JsonSerializer<Date>{
	 private static DateTimeFormatter formatter =
	            DateTimeFormat.forPattern("MM-dd-yyyy-hh-mm-ss");
	    @Override
	    public void serialize(Date date, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
	            throws IOException, JsonProcessingException {
	        jsonGenerator.writeString(formatter.print(date.getTime()));
	 
	    }
}
