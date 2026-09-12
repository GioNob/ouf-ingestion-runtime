package it.comune.trieste.ouf.ingestion;
import java.util.*;
public record ExecutionBundle(String bundleId,String bundleVersion,String checksum,String sourceId,String acquisitionMode,String bindingRef,Map<String,Object> configuration){public ExecutionBundle{configuration=Map.copyOf(configuration);if(bundleId==null||checksum==null||sourceId==null)throw new IllegalArgumentException("ING_BUNDLE_INVALID");}}
