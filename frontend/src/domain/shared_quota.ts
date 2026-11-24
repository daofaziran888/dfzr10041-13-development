/**
 * 跨来源共享额度采集的纯前端领域逻辑。
 *
 * 与后端 `app/core/shared_quota.py` 保持同一口径，供工作台预览本轮名额分配：
 * 每轮总 3 个名额，每个来源 1 个基础名额，其余为共享名额；某来源当前批次没有
 * 合格原行时，它的基础名额可以借给另一来源。借用冲突由结算口径决定：
 * - borrow_on_claim：借用一经领用即生效，迟到来源顺延到下一轮。
 * - reserve_until_submit：提交前保留来源保底额度，收回借用、迟到来源先写。
 *
 * 静态预览只处理“此刻各来源还有多少合格原行”；迟到原行引起的动态冲突用
 * `resolveBorrowConflict` 单独结算。
 */

export type BorrowPolicy = 'borrow_on_claim' | 'reserve_until_submit'

export type SlotKind = 'base' | 'shared' | 'borrowed'

export type SourceBatch = {
  key: string
  order: number
  /** 当前批次里合格原行的条数（空行/错行不计）。 */
  qualified: number
}

export type SlotAllocation = {
  sourceKey: string
  slot: SlotKind
  donorKey: string | null
}

export type RoundPlan = {
  allocations: SlotAllocation[]
  /** 还有合格原行但本轮没拿到名额的来源，按来源顺序。 */
  deferred: string[]
  /** 被收回借用、顺延到下一轮的借用方。 */
  preempted: string[]
}

export type BorrowConflict = {
  /** 借出基础名额的来源（迟到方）。 */
  donorKey: string
  /** 借用方。 */
  borrowerKey: string
  policy: BorrowPolicy
}

export type ConflictResolution = {
  write: string
  defer: string
  /** 借用是否被收回（仅 reserve_until_submit 会收回）。 */
  preempted: boolean
}

const TOTAL_QUOTA = 3
const BASE_QUOTA = 1

/**
 * 为一批来源规划本轮名额分配。
 *
 * 1. 按来源顺序，每个有合格原行的来源先取一个基础名额；
 * 2. 剩余名额作为共享名额，按来源顺序循环发给仍有合格原行的来源；
 * 3. 共享名额用尽后，才借用“本轮没有合格原行”的来源的基础名额。
 */
export function planRound(
  batches: SourceBatch[],
  _policy: BorrowPolicy,
  options: { totalQuota?: number; baseQuota?: number } = {},
): RoundPlan {
  const totalQuota = options.totalQuota ?? TOTAL_QUOTA
  const baseQuota = options.baseQuota ?? BASE_QUOTA
  const ordered = [...batches].sort((a, b) => a.order - b.order)
  const remaining = new Map(ordered.map((item) => [item.key, Math.max(0, item.qualified)]))
  const allocations: SlotAllocation[] = []
  const baseOwner = new Set<string>()
  const loanedFrom = new Set<string>()
  const preempted: string[] = []

  const take = (sourceKey: string, slot: SlotKind, donorKey: string | null): boolean => {
    const left = remaining.get(sourceKey) ?? 0
    if (left <= 0) return false
    remaining.set(sourceKey, left - 1)
    allocations.push({ sourceKey, slot, donorKey })
    if (slot === 'base') baseOwner.add(sourceKey)
    if (slot === 'borrowed' && donorKey) loanedFrom.add(donorKey)
    return true
  }

  // 1. 基础名额。
  for (const item of ordered) {
    if (allocations.length >= totalQuota) break
    if (baseQuota >= 1) take(item.key, 'base', null)
  }

  // 2. 共享名额：总数减去所有来源的基础名额，按来源顺序循环。
  let sharedLeft = Math.max(totalQuota - baseQuota * ordered.length, 0)
  let progressed = true
  while (progressed && sharedLeft > 0) {
    progressed = false
    for (const item of ordered) {
      if (sharedLeft <= 0) break
      if ((remaining.get(item.key) ?? 0) <= 0) continue
      if (take(item.key, 'shared', null)) {
        sharedLeft -= 1
        progressed = true
      }
    }
  }

  // 3. 借用：共享名额已满，向本轮没有合格原行的来源借基础名额。
  for (const item of ordered) {
    if (allocations.length >= totalQuota) break
    if ((remaining.get(item.key) ?? 0) <= 0) continue
    const donor = ordered.find(
      (candidate) =>
        candidate.key !== item.key &&
        !baseOwner.has(candidate.key) &&
        !loanedFrom.has(candidate.key) &&
        (candidate.qualified ?? 0) <= 0,
    )
    if (donor) take(item.key, 'borrowed', donor.key)
  }

  const deferred = ordered
    .filter((item) => (remaining.get(item.key) ?? 0) > 0)
    .map((item) => item.key)

  return { allocations, deferred, preempted }
}

/**
 * 结算“借用已领用、迟到来源基础名额到齐”的冲突。
 *
 * - borrow_on_claim：借用生效，迟到来源下一轮再写；
 * - reserve_until_submit：借用尚未提交，保留迟到来源保底额度，收回借用。
 */
export function resolveBorrowConflict(conflict: BorrowConflict): ConflictResolution {
  if (conflict.policy === 'borrow_on_claim') {
    return { write: conflict.borrowerKey, defer: conflict.donorKey, preempted: false }
  }
  return { write: conflict.donorKey, defer: conflict.borrowerKey, preempted: true }
}

/** 按来源汇总本轮实际写入的原行数。 */
export function writtenCounts(plan: RoundPlan): Record<string, number> {
  const counts: Record<string, number> = {}
  for (const row of plan.allocations) {
    counts[row.sourceKey] = (counts[row.sourceKey] ?? 0) + 1
  }
  return counts
}
