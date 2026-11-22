package it.comune.trieste.ouf.ingestion;
import java.util.*;import java.nio.file.*;import com.fasterxml.jackson.databind.*;
public class ReturnFixture {
 public static void main(String[] args)throws Exception {
  var json=new ObjectMapper(); int i=0;
  for(var item:json.readTree(Path.of(".task43-check/fixture.json").toFile())){
   Map<String,Object> e=json.convertValue(item.get("envelope"),Map.class), row=json.convertValue(item.get("row"),Map.class);
   var a=PublishedActivation.decode(e,"tenant-a",json);var b=PublishedExecutionMapper.map(a.execution());
   var mapper=new ConfiguredValueMapping(b.configuration(),ValuePins.EMPTY);
   var mapped=mapper.apply(row,1);var expected=new HashMap<String,Object>();
   expected.put("https://example.test/name",List.of("Library","Station","Museum").get(i));
   if(i==1)expected.put("https://example.test/latitude","45.650123456789012345");
   if(!expected.equals(mapped))throw new AssertionError("fixture "+i);i++;
  }
  if(i!=3)throw new AssertionError();System.out.println("PASS three predecessor fixture publications, exact candidate latitude");
 }
}