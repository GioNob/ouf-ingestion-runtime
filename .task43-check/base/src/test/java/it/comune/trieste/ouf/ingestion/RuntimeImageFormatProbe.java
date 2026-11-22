package it.comune.trieste.ouf.ingestion;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.sql.DriverManager;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/** CI-only image probe: never included in the deployable application. */
public final class RuntimeImageFormatProbe {
  public static void main(String[] args) throws Exception {
    var path=Files.createTempFile("ouf-native-probe-", ".sqlite");
    try {
      Class.forName("org.sqlite.JDBC");
      try(var connection=DriverManager.getConnection("jdbc:sqlite:"+path)) {
        try(var statement=connection.createStatement()){
          statement.execute("create table probe (id integer primary key, label text not null)");
          statement.execute("insert into probe values (1,'native-format-ok')");
          try(var result=statement.executeQuery("select label from probe where id=1")){
            if(!result.next()||!"native-format-ok".equals(result.getString(1)))throw new AssertionError("SQLITE_NATIVE_PROBE_FAILED");
          }
        }
      }
    } finally { Files.deleteIfExists(path); }
    byte[] bytes;
    try(var book=new XSSFWorkbook();var output=new ByteArrayOutputStream()){
      var row=book.createSheet("dati").createRow(0);
      row.createCell(0).setCellValue("00123");
      row.createCell(1).setCellValue(42);
      book.write(output);bytes=output.toByteArray();
    }
    try(var book=new XSSFWorkbook(new ByteArrayInputStream(bytes))){
      var row=book.getSheetAt(0).getRow(0);var format=new DataFormatter(java.util.Locale.ROOT);
      if(!"00123".equals(format.formatCellValue(row.getCell(0)))||!"42".equals(format.formatCellValue(row.getCell(1))))
        throw new AssertionError("XLSX_FORMAT_PROBE_FAILED");
    }
    System.out.println("RUNTIME_IMAGE_SQLITE_AND_XLSX=PASS");
  }
}
