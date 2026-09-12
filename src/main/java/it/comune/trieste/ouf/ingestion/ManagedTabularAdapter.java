package it.comune.trieste.ouf.ingestion;

import static it.comune.trieste.ouf.ingestion.AdapterSpi.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.commons.csv.*;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/** INTERNAL_MANAGED CSV/XLSX adapter. It never evaluates spreadsheet formulas. */
public final class ManagedTabularAdapter implements AdapterSpi {
  private final RuntimePorts.ManagedObjectPort objects;
  public ManagedTabularAdapter(RuntimePorts.ManagedObjectPort objects){this.objects=Objects.requireNonNull(objects);}
  public String adapterId(){return "managed-tabular-v1";}
  public Set<String> acquisitionModes(){return Set.of("INTERNAL_MANAGED_CSV","INTERNAL_MANAGED_XLSX");}
  public Compatibility compatibility(){return new Compatibility("1.0.0","1.0.0");}

  public RecordCursor open(ExecutionBundle bundle,Checkpoint checkpoint){
    Map<String,Object> c=bundle.configuration();
    long expectedSize=number(c,"expectedSize",1,50_000_000);
    String expectedHash=required(c,"expectedHash");
    byte[] bytes=objects.read(bundle.bindingRef(),expectedSize,expectedHash);
    if(bytes.length!=expectedSize) throw error("ING_MANAGED_SIZE_MISMATCH",ErrorClass.DATA);
    Limits limits=new Limits((int)number(c,"maxRows",1,1_000_000),(int)number(c,"maxColumns",1,10_000),(int)number(c,"maxCellChars",1,1_000_000));
    List<String> keys=strings(c.get("identityFields"));
    long resume=checkpoint==null?0:number(checkpoint.value(),"ordinal",0,Integer.MAX_VALUE);
    List<SourceRecord> rows=switch(bundle.acquisitionMode()){
      case "INTERNAL_MANAGED_CSV" -> csv(bundle,bytes,keys,limits);
      case "INTERNAL_MANAGED_XLSX" -> xlsx(bundle,bytes,keys,limits,required(c,"sheet"));
      default -> throw error("ING_MANAGED_MODE_UNSUPPORTED",ErrorClass.CONFIGURATION);
    };
    if(resume>rows.size()) throw error("ING_CHECKPOINT_OUT_OF_RANGE",ErrorClass.CONFIGURATION);
    return new Cursor(rows,(int)resume);
  }

  private List<SourceRecord> csv(ExecutionBundle b,byte[] bytes,List<String> keys,Limits limits){
    try(Reader reader=new InputStreamReader(new ByteArrayInputStream(bytes),StandardCharsets.UTF_8)){
      CSVFormat format=CSVFormat.RFC4180.builder().setHeader().setSkipHeaderRecord(true).setDuplicateHeaderMode(DuplicateHeaderMode.DISALLOW).get();
      List<SourceRecord> out=new ArrayList<>(); Set<String> ids=new HashSet<>();
      for(CSVRecord record:format.parse(reader)){
        if(record.size()>limits.columns)throw error("ING_MANAGED_TOO_MANY_COLUMNS",ErrorClass.DATA);
        Map<String,Object> row=new LinkedHashMap<>(); record.toMap().forEach((k,v)->row.put(k,checked(v,limits)));
        append(b,out,ids,row,"csv",record.getRecordNumber(),keys,limits);
      } return List.copyOf(out);
    }catch(AdapterException e){throw e;}catch(IOException|IllegalArgumentException e){throw error("ING_MANAGED_CSV_INVALID",ErrorClass.DATA);}
  }

  private List<SourceRecord> xlsx(ExecutionBundle b,byte[] bytes,List<String> keys,Limits limits,String sheetName){
    try(Workbook workbook=new XSSFWorkbook(new ByteArrayInputStream(bytes))){
      Sheet sheet=workbook.getSheet(sheetName); if(sheet==null)throw error("ING_MANAGED_SHEET_MISSING",ErrorClass.CONFIGURATION);
      Iterator<Row> it=sheet.rowIterator(); if(!it.hasNext())return List.of();
      List<String> headers=headers(it.next(),limits); List<SourceRecord> out=new ArrayList<>(); Set<String> ids=new HashSet<>();
      while(it.hasNext()){
        Row r=it.next(); Map<String,Object> row=new LinkedHashMap<>();
        for(int i=0;i<headers.size();i++)row.put(headers.get(i),cell(r.getCell(i,Row.MissingCellPolicy.RETURN_BLANK_AS_NULL),limits));
        append(b,out,ids,row,sheetName,r.getRowNum(),keys,limits);
      } return List.copyOf(out);
    }catch(AdapterException e){throw e;}catch(IOException|RuntimeException e){throw error("ING_MANAGED_XLSX_INVALID",ErrorClass.DATA);}
  }

  private static List<String> headers(Row row,Limits limits){
    if(row.getLastCellNum()<1||row.getLastCellNum()>limits.columns)throw error("ING_MANAGED_HEADER_INVALID",ErrorClass.DATA);
    List<String> h=new ArrayList<>();Set<String> unique=new HashSet<>();
    for(int i=0;i<row.getLastCellNum();i++){String v=String.valueOf(cell(row.getCell(i),limits)).strip();if(v.isEmpty()||!unique.add(v))throw error("ING_MANAGED_HEADER_INVALID",ErrorClass.DATA);h.add(v);}return h;
  }
  private static Object cell(Cell c,Limits limits){
    if(c==null)return null; if(c.getCellType()==CellType.FORMULA)throw error("ING_MANAGED_FORMULA_FORBIDDEN",ErrorClass.DATA);
    return switch(c.getCellType()){case STRING->checked(c.getStringCellValue(),limits);case NUMERIC->c.getNumericCellValue();case BOOLEAN->c.getBooleanCellValue();case BLANK->null;case ERROR->throw error("ING_MANAGED_CELL_ERROR",ErrorClass.DATA);default->checked(c.toString(),limits);};
  }
  private static String checked(String v,Limits l){if(v.length()>l.cellChars)throw error("ING_MANAGED_CELL_TOO_LARGE",ErrorClass.DATA);return v;}
  private static void append(ExecutionBundle b,List<SourceRecord> out,Set<String> ids,Map<String,Object> row,String sheet,long ordinal,List<String> keys,Limits limits){
    if(out.size()>=limits.rows)throw error("ING_MANAGED_TOO_MANY_ROWS",ErrorClass.DATA);
    String id=ManagedIdentity.sourceObjectId(b.sourceId(),required(b.configuration(),"expectedHash"),sheet,ordinal,row,keys);
    if(!ids.add(id))throw error("ING_MANAGED_DUPLICATE_IDENTITY",ErrorClass.DATA);
    out.add(new SourceRecord(id,ordinal,row,Map.of("managedObjectRef",b.bindingRef(),"sheet",sheet,"rowOrdinal",ordinal)));
  }
  private static String required(Map<String,Object> c,String k){Object v=c.get(k);if(v==null||String.valueOf(v).isBlank())throw error("ING_MANAGED_CONFIG_MISSING",ErrorClass.CONFIGURATION);return String.valueOf(v);}
  private static long number(Map<String,Object> c,String k,long min,long max){Object v=c.get(k);if(!(v instanceof Number n)||n.longValue()<min||n.longValue()>max)throw error("ING_MANAGED_CONFIG_INVALID",ErrorClass.CONFIGURATION);return n.longValue();}
  private static List<String> strings(Object v){if(v==null)return List.of();if(!(v instanceof Collection<?> x))throw error("ING_MANAGED_CONFIG_INVALID",ErrorClass.CONFIGURATION);return x.stream().map(String::valueOf).toList();}
  private static AdapterException error(String code,ErrorClass cls){return new AdapterException(code,cls,code);}
  private record Limits(int rows,int columns,int cellChars){}
  private static final class Cursor implements RecordCursor{private final List<SourceRecord> rows;private int position;Cursor(List<SourceRecord> rows,int position){this.rows=rows;this.position=position;}public Optional<SourceRecord> next(){return position<rows.size()?Optional.of(rows.get(position++)):Optional.empty();}public Checkpoint checkpoint(){return new Checkpoint(Map.of("ordinal",position));}public void close(){}}
}
