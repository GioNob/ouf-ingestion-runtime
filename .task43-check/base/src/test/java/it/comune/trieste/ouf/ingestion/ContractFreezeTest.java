package it.comune.trieste.ouf.ingestion;
import static org.assertj.core.api.Assertions.assertThat;
import java.nio.file.*;import java.security.*;import java.util.*;import org.junit.jupiter.api.Test;
class ContractFreezeTest {
 @Test void rc3CopiesMatchPinnedChecksums() throws Exception {for(String line:Files.readAllLines(Path.of("contracts/RC3_SHA256SUMS.txt"))){if(line.isBlank())continue;String[] p=line.split("  ",2);byte[] bytes=Files.readAllBytes(Path.of(p[1]));assertThat(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))).isEqualTo(p[0]);}}
 @Test void executionBundleRejectsMissingAuthorityFields(){org.assertj.core.api.Assertions.assertThatThrownBy(()->new ExecutionBundle(null,"1","sha256:x","s","MANAGED",null,Map.of())).hasMessage("ING_BUNDLE_INVALID");}
}
