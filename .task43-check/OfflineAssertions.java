package it.comune.trieste.ouf.ingestion;
import java.util.*;
final class OfflineAssertions {
 static int checks;
 static void assertEquals(Object a,Object b){checks++;if(!Objects.equals(a,b))throw new AssertionError(a+" != "+b);}
 static void assertNull(Object o){assertEquals(null,o);}
 static void assertNotNull(Object o){checks++;if(o==null)throw new AssertionError("null");}
 static <T extends Throwable>T assertThrows(Class<T> c,Runnable action){
   checks++;try{action.run();}catch(Throwable x){if(c.isInstance(x))return c.cast(x);throw new AssertionError(x);}
   throw new AssertionError("Expected "+c);
 }
}
