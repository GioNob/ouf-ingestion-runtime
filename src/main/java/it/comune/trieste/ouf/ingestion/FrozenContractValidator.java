package it.comune.trieste.ouf.ingestion;

import com.fasterxml.jackson.databind.*;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.time.OffsetDateTime;
import java.util.*;
import org.springframework.stereotype.Component;

/** Small fail-closed validator for the JSON-Schema keywords used by the frozen rc3 contracts. */
@Component
public final class FrozenContractValidator {
  private final ObjectMapper json;
  public FrozenContractValidator(ObjectMapper json){this.json=json;}

  public void validate(Path schemaPath,Object value){
    try{validateNode(json.readTree(schemaPath.toFile()),json.valueToTree(value),"$",schemaPath.getParent());}
    catch(IOException e){throw new ContractViolation("ING_CONTRACT_SCHEMA_UNAVAILABLE",List.of("$"));}
  }
  public void validate(String classpathResource,Object value){
    try(InputStream in=getClass().getResourceAsStream(classpathResource)){
      if(in==null)throw new IOException("missing");
      validateNode(json.readTree(in),json.valueToTree(value),"$",null);
    }catch(IOException e){throw new ContractViolation("ING_CONTRACT_SCHEMA_UNAVAILABLE",List.of("$"));}
  }

  private void validateNode(JsonNode schema,JsonNode value,String path,Path base){
    if(schema.has("$ref")){
      if(base==null)throw new ContractViolation("ING_CONTRACT_REF_UNAVAILABLE",List.of(path));
      Path ref=base.resolve(schema.get("$ref").asText()).normalize();
      try{validateNode(json.readTree(ref.toFile()),value,path,ref.getParent());return;}
      catch(IOException e){throw new ContractViolation("ING_CONTRACT_REF_UNAVAILABLE",List.of(path));}
    }
    if(schema.has("oneOf")){
      int valid=0;for(JsonNode option:schema.get("oneOf"))try{validateNode(option,value,path,base);valid++;}catch(ContractViolation ignored){}
      if(valid!=1)fail(path);return;
    }
    if(schema.has("type")&&!matchesType(schema.get("type"),value))fail(path);
    if(schema.has("const")&&!schema.get("const").equals(value))fail(path);
    if(schema.has("enum")){boolean found=false;for(JsonNode item:schema.get("enum"))found|=item.equals(value);if(!found)fail(path);}
    if(value.isTextual()&&schema.path("minLength").isInt()&&value.textValue().length()<schema.get("minLength").intValue())fail(path);
    if(value.isNumber()&&schema.has("minimum")&&value.decimalValue().compareTo(schema.get("minimum").decimalValue())<0)fail(path);
    if(value.isTextual()&&"date-time".equals(schema.path("format").asText()))try{OffsetDateTime.parse(value.textValue());}catch(Exception e){fail(path);}
    if(value.isObject())validateObject(schema,value,path,base);
    if(value.isArray())validateArray(schema,value,path,base);
    apply(schema.get("allOf"),value,path,base);
    if(schema.has("if")&&isValid(schema.get("if"),value,path,base)&&schema.has("then"))validateNode(schema.get("then"),value,path,base);
    if(schema.has("anyOf")){boolean valid=false;for(JsonNode option:schema.get("anyOf"))valid|=isValid(option,value,path,base);if(!valid)fail(path);}
  }
  private void validateObject(JsonNode schema,JsonNode value,String path,Path base){
    if(schema.has("required"))for(JsonNode required:schema.get("required"))if(!value.has(required.asText()))fail(path+"."+required.asText());
    JsonNode properties=schema.get("properties");
    if(properties!=null){Iterator<String> names=properties.fieldNames();while(names.hasNext()){String n=names.next();if(value.has(n))validateNode(properties.get(n),value.get(n),path+"."+n,base);}}
    if(schema.path("additionalProperties").isBoolean()&&!schema.get("additionalProperties").booleanValue()&&properties!=null){Iterator<String> names=value.fieldNames();while(names.hasNext()){String n=names.next();if(!properties.has(n))fail(path+"."+n);}}
  }
  private void validateArray(JsonNode schema,JsonNode value,String path,Path base){
    if(schema.has("minItems")&&value.size()<schema.get("minItems").intValue())fail(path);
    if(schema.path("uniqueItems").asBoolean(false)){Set<JsonNode> seen=new HashSet<>();for(JsonNode item:value)if(!seen.add(item))fail(path);}
    if(schema.has("items"))for(int i=0;i<value.size();i++)validateNode(schema.get("items"),value.get(i),path+'['+String.valueOf(i)+']',base);
  }
  private void apply(JsonNode alternatives,JsonNode value,String path,Path base){if(alternatives!=null)for(JsonNode item:alternatives)validateNode(item,value,path,base);}
  private boolean isValid(JsonNode schema,JsonNode value,String path,Path base){try{validateNode(schema,value,path,base);return true;}catch(ContractViolation e){return false;}}
  private boolean matchesType(JsonNode type,JsonNode v){if(type.isArray()){for(JsonNode t:type)if(matches(t.asText(),v))return true;return false;}return matches(type.asText(),v);}
  private boolean matches(String t,JsonNode v){return switch(t){case "object"->v.isObject();case "array"->v.isArray();case "string"->v.isTextual();case "number"->v.isNumber();case "integer"->v.isIntegralNumber();case "boolean"->v.isBoolean();case "null"->v.isNull();default->false;};}
  private static void fail(String path){throw new ContractViolation("ING_CONTRACT_INVALID",List.of(path));}
  public static final class ContractViolation extends RuntimeException {
    private final String safeCode;private final List<String> paths;
    public ContractViolation(String safeCode,List<String> paths){super(safeCode);this.safeCode=safeCode;this.paths=List.copyOf(paths);}
    public String safeCode(){return safeCode;}public List<String> paths(){return paths;}
  }
}
