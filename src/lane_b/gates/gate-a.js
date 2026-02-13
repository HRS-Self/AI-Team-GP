import { warnDeprecatedOnce } from "../../utils/deprecation.js";
import { requestApplyApproval, approveApplyApproval, rejectApplyApproval } from "./apply-approval.js";

export async function requestGateA({ workId, dryRun = false } = {}) {
  warnDeprecatedOnce("gate-a", "`--gate-a` is deprecated; use `--apply-approval`.");
  return await requestApplyApproval({ workId, dryRun });
}

export async function approveGateA({ workId, approvedBy = "human", notes = null } = {}) {
  warnDeprecatedOnce("gate-a-approve", "`--gate-a-approve` is deprecated; use `--apply-approve`.");
  return await approveApplyApproval({ workId, approvedBy, notes });
}

export async function rejectGateA({ workId, approvedBy = "human", notes = null } = {}) {
  warnDeprecatedOnce("gate-a-reject", "`--gate-a-reject` is deprecated; use `--apply-reject`.");
  return await rejectApplyApproval({ workId, approvedBy, notes });
}
