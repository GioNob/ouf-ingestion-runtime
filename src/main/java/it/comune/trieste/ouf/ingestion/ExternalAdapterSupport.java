package it.comune.trieste.ouf.ingestion;

import static it.comune.trieste.ouf.ingestion.AdapterSpi.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

final class ExternalAdapterSupport {
  private ExternalAdapterSupport(){}
  static void requireGateway(ExecutionBundle b){if(b.bindingRef()==null||!b.bindingRef().startsWith("gateway://"))throw error("ING_GATEWAY_BINDING_REQUIRED",ErrorClass.CONFIGURATION);for(var e:b.configuration().entrySet())if(e.getKey().toLowerCase(Locale.ROOT).matches(".*(url|credential|password|secret|token).*"))throw error("ING_RAW_ROUTE_FORBIDDEN",ErrorClass.CONFIGURATION);}
  static String identity(String source,String type,Map<String,Object> row,List<String> fields){if(fields.isEmpty())throw error("ING_IDENTITY_FIELDS_REQUIRED",ErrorClass.CONFIGURATION);StringBuilder material=new StringBuilder(source).append('\u001f').append(type);for(String field:fields){Object value=path(row,field);if(value==null||String.valueOf(value).isBlank())throw error("ING_SOURCE_IDENTITY_MISSING",ErrorClass.DATA);material.append('\u001f').append(field).append('=').append(String.valueOf(value).strip());}try{return "native:"+HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(material.toString().getBytes(StandardCharsets.UTF_8)));}catch(Exception e){throw new IllegalStateException(e);}}
  @SuppressWarnings("unchecked") static Object path(Map<String,Object> value,String path){Object cursor=value;for(String part:path.split("\\.")){if(!(cursor instanceof Map<?,?> map))return null;cursor=((Map<String,Object>)map).get(part);}return cursor;}
  static int integer(Map<String,Object> c,String name,int fallback,int min,int max){Object raw=c.getOrDefault(name,fallback);if(!(raw instanceof Number n)||n.intValue()<min||n.intValue()>max)throw error("ING_ADAPTER_LIMIT_INVALID",ErrorClass.CONFIGURATION);return n.intValue();}
  static String text(Map<String,Object> c,String name,String fallback){Object raw=c.getOrDefault(name,fallback);if(!(raw instanceof String s)||s.isBlank())throw error("ING_ADAPTER_CONFIG_INVALID",ErrorClass.CONFIGURATION);return s;}
  static List<String> strings(Map<String,Object> c,String name){Object raw=c.get(name);if(!(raw instanceof Collection<?> values)||values.isEmpty())throw error("ING_ADAPTER_CONFIG_INVALID",ErrorClass.CONFIGURATION);return values.stream().map(String::valueOf).toList();}
  static AdapterException error(String code,ErrorClass cls){return new AdapterException(code,cls,code);}
}
