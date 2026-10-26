package it.comune.trieste.ouf.ingestion;

import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import it.comune.trieste.ouf.authorization.TestAuthorization;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class AcquisitionEvidenceApiTest {
  @Test void closedDecoderRejectsDuplicateUnknownNullCoercedAndOversizedRequestsBeforeServiceIo()throws Exception{
    var service=mock(AcquisitionEvidenceService.class);
    var http=MockMvcBuilders.standaloneSetup(new AcquisitionEvidenceApi(service,new TrustedAuthorizationContext())).build();
    String id=UUID.randomUUID().toString(),valid="{\"sourceId\":\"s\",\"runId\":\""+id+"\"}";
    for(String body:List.of("{}",valid+"{}",valid.replace("\"s\"","null"),valid.replace("\"s\"","4"),
        valid.replace("\"sourceId\"","\"sourceId\":\"other\",\"sourceId\""),valid.replace("}",",\"unknown\":true}"),
        valid.replace("}",",\"limit\":1.5}"),valid.replace("}",",\"limit\":\"1\"}"),valid.replace("}",",\"limit\":101}"),
        valid.replace("}",",\"limit\":0}"),valid.replace("}",",\"afterLineageId\":null}"),valid.replace(id,"00000000-0000-0000-0000-000000000000"),
        valid.replace("\"s\"","\""+"s".repeat(4097)+"\"")))
      http.perform(post("/api/internal/v1/ingestion/operations/acquisition").contentType("application/json").content(body)).andExpect(status().isBadRequest());
    verifyNoInteractions(service);
    when(service.read(any(),any())).thenReturn(Map.of("partial",false));
    http.perform(post("/api/internal/v1/ingestion/operations/acquisition").contentType("application/json").content(valid)
      .with(r->{TestAuthorization.bind(r,"reader","HUMAN",AcquisitionEvidenceService.CAPABILITIES);return r;}))
      .andExpect(status().isOk()).andExpect(jsonPath("$.partial").value(false));
    verify(service).read(eq(new AcquisitionEvidenceApi.Query("s",UUID.fromString(id),50,null)),any());
  }
}
