import { warnDeprecatedOnce } from "../../utils/deprecation.js";
import { requestMergeApproval, approveMergeApproval, rejectMergeApproval } from "./merge-approval.js";

export async function requestGateB({ workId, dryRun = false } = {}) {
  warnDeprecatedOnce("gate-b", "`--gate-b` is deprecated; use `--merge-approval`.");
  return await requestMergeApproval({ workId, dryRun });
}

export async function approveGateB({ workId, approvedBy = "human", notes = null } = {}) {
  warnDeprecatedOnce("gate-b-approve", "`--gate-b-approve` is deprecated; use `--merge-approve`.");
  return await approveMergeApproval({ workId, approvedBy, notes });
}

export async function rejectGateB({ workId, approvedBy = "human", notes = null } = {}) {
  warnDeprecatedOnce("gate-b-reject", "`--gate-b-reject` is deprecated; use `--merge-reject`.");
  return await rejectMergeApproval({ workId, approvedBy, notes });
}
