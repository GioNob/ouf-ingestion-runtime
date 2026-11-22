package it.comune.trieste.ouf.ingestion;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.*;
@RestControllerAdvice
public class OwnerAuthorizationErrors {
 @ExceptionHandler(SecurityException.class) ResponseEntity<ProblemDetail> denied(SecurityException error){return ResponseEntity.status(403).body(ProblemDetail.forStatusAndDetail(HttpStatus.FORBIDDEN,"Owner authorization denied"));}
}
