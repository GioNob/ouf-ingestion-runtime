package it.comune.trieste.ouf.ingestion;

import java.util.Map;

/** The owner decision, independent of the reader's authority to submit a command. */
enum RecoveryAction {
  NONE,
  AUTOMATIC_RETRY,
  AUTOMATIC_RECONCILIATION,
  HUMAN_RESUME_OR_ABORT,
  HUMAN_ABORT_REQUIRED,
  HUMAN_QUARANTINE_REVIEW,
  HUMAN_SCHEDULE_REVIEW,
  WAITING_FOR_PUBLICATION_VERIFICATION,
  NEW_PUBLICATION_REQUIRED,
  TERMINAL;

  boolean requiresHuman() {
    return name().startsWith("HUMAN_");
  }

  enum RunState {
    READY, PREFLIGHT, RUNNING, DRAINING, SUCCEEDED, COMPLETED_WITH_WARNINGS, FAILED, PAUSED, ABORTED;

    boolean terminal() {
      return switch (this) {
        case SUCCEEDED, COMPLETED_WITH_WARNINGS, FAILED, ABORTED -> true;
        case READY, PREFLIGHT, RUNNING, DRAINING, PAUSED -> false;
      };
    }
  }

  enum ResumeBlock {
    NONE, QUARANTINE, LAKE, PUBLICATION, DISCOVERY, HANDOFF, SNAPSHOT
  }

  static ResumeBlock resumeBlock(Map<String, Object> facts) {
    if (!flag(facts,"has_snapshot")) return ResumeBlock.SNAPSHOT;
    if (flag(facts,"quarantine_not_ready")) return ResumeBlock.QUARANTINE;
    if ("ABORT_REQUIRED".equals(facts.get("lake_next_action"))) return ResumeBlock.LAKE;
    if (flag(facts,"publication_required")) {
      if (!flag(facts,"publication_current")) return ResumeBlock.PUBLICATION;
      if (!flag(facts,"publication_fresh")) return ResumeBlock.DISCOVERY;
    }
    if (flag(facts,"handoff_recovery") && !flag(facts,"handoff_renewable")) return ResumeBlock.HANDOFF;
    return ResumeBlock.NONE;
  }

  static RecoveryAction paused(Map<String, Object> facts) {
    return switch (resumeBlock(facts)) {
      case NONE -> HUMAN_RESUME_OR_ABORT;
      case QUARANTINE -> HUMAN_QUARANTINE_REVIEW;
      case LAKE, HANDOFF, SNAPSHOT -> HUMAN_ABORT_REQUIRED;
      case PUBLICATION -> NEW_PUBLICATION_REQUIRED;
      case DISCOVERY -> WAITING_FOR_PUBLICATION_VERIFICATION;
    };
  }

  static boolean flag(Map<String, Object> facts, String key) {
    return Boolean.TRUE.equals(facts.get(key));
  }
}
