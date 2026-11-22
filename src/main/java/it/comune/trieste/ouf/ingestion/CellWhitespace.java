package it.comune.trieste.ouf.ingestion;

/** The Unicode White_Space set fixed by ouf.value-types.v1. */
final class CellWhitespace {
  private CellWhitespace() {}

  static boolean contains(char c) {
    return (c >= 0x0009 && c <= 0x000D) || (c >= 0x2000 && c <= 0x200A)
        || c == 0x0020 || c == 0x0085 || c == 0x00A0 || c == 0x1680
        || c == 0x2028 || c == 0x2029 || c == 0x202F || c == 0x205F || c == 0x3000;
  }

  static String trim(String cell) {
    int first = 0, last = cell.length();
    while (first < last && contains(cell.charAt(first))) first++;
    while (last > first && contains(cell.charAt(last - 1))) last--;
    return cell.substring(first, last);
  }
}
