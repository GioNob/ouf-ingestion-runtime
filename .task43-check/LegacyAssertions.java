package it.comune.trieste.ouf.ingestion;
import java.util.*;
class LegacyAssertions {
 static int n;
 final Object v;LegacyAssertions(Object v){this.v=v;}
 static LegacyAssertions assertThat(Object v){return new LegacyAssertions(v);}
 static LegacyAssertions assertThatThrownBy(Runnable r){try{r.run();}catch(Exception x){return assertThat(x);}throw new AssertionError("expected exception");}
 void isEqualTo(Object expected){n++;if(!Objects.equals(v,expected))throw new AssertionError();}
 void isSameAs(Object expected){n++;if(v!=expected)throw new AssertionError();}
 LegacyAssertions isInstanceOf(Class<?> c){n++;if(!c.isInstance(v))throw new AssertionError();return this;}
 LegacyAssertions hasMessage(String s){n++;if(!Objects.equals(s,((Throwable)v).getMessage()))throw new AssertionError();return this;}
 void hasNoCause(){n++;if(((Throwable)v).getCause()!=null)throw new AssertionError();}
 void isNull(){n++;if(v!=null)throw new AssertionError();}
}