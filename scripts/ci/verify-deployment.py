#!/usr/bin/env python3
import pathlib, sys
rendered=pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
required={"digest":"@sha256:","startup":"startupProbe:","liveness":"livenessProbe:","readiness":"readinessProbe:","grace":"terminationGracePeriodSeconds:","drain":"preStop:","non-root":"runAsNonRoot: true","read-only":"readOnlyRootFilesystem: true","network":"kind: NetworkPolicy","pdb":"kind: PodDisruptionBudget","slo alerts":"kind: PrometheusRule"}
missing=[name for name,marker in required.items() if marker not in rendered]
application=pathlib.Path("src/main/resources/application.yml").read_text(encoding="utf-8");docker=pathlib.Path("Dockerfile").read_text(encoding="utf-8")
if "shutdown: graceful" not in application or "timeout-per-shutdown-phase:" not in application or "probes.enabled: true" not in application or "STOPSIGNAL SIGTERM" not in docker: missing.append("runtime-shutdown")
if missing: raise SystemExit("deployment contract missing: "+", ".join(missing))
print("deployment contract verified")
