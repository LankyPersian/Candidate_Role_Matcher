// ============================================
// COST TRACKING UTILITY
// ============================================
// Tracks API usage and enforces daily cost limits
// Prevents API bill explosions

import { logger } from "@trigger.dev/sdk";
import {
  ENV,
  COST_LIMITS,
  GEMINI_CONFIG,
  formatCost,
  getCostWarningLevel,
  wouldExceedCostLimit,
  wouldExceedAPILimit,
} from "./config";

// ============================================
// TYPES
// ============================================

interface APIUsageRecord {
  id?: string;
  date: string; // YYYY-MM-DD
  api_name: "gemini" | "ghl";
  operation_type: string;
  calls_count: number;
  tokens_used?: number;
  cost_usd: number;
  batch_id?: string;
  created_at?: string;
}

interface DailyUsageSummary {
  date: string;
  total_calls: number;
  total_cost_usd: number;
  gemini_calls: number;
  gemini_cost_usd: number;
  ghl_calls: number;
  remaining_calls: number;
  remaining_budget_usd: number;
  warning_level: "ok" | "warning" | "critical" | "exceeded";
  can_process_more: boolean;
}

interface CostEstimate {
  estimated_calls: number;
  estimated_cost_usd: number;
  would_exceed_limit: boolean;
  remaining_budget_usd: number;
  warning_level: "ok" | "warning" | "critical" | "exceeded";
}

// ============================================
// DATABASE OPERATIONS
// ============================================

/**
 * Record API usage in database
 */
export async function recordAPIUsage(usage: Omit<APIUsageRecord, "id" | "created_at">): Promise<void> {
  try {
    const response = await fetch(`${ENV.SUPABASE_URL}/rest/v1/api_usage`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
        apikey: ENV.SUPABASE_SERVICE_KEY,
        "Content-Type": "application/json",
        Prefer: "return=minimal",
      },
      body: JSON.stringify(usage),
    });

    if (!response.ok) {
      const errorText = await response.text();
      logger.warn("Failed to record API usage", {
        status: response.status,
        error: errorText.slice(0, 500),
      });
      // Don't throw - this is non-critical
    }
  } catch (error: any) {
    logger.warn("Exception recording API usage", {
      error: error.message,
    });
    // Don't throw - this is non-critical
  }
}

/**
 * Get today's usage summary
 */
export async function getTodayUsageSummary(): Promise<DailyUsageSummary> {
  const today = new Date().toISOString().split("T")[0]; // YYYY-MM-DD

  try {
    const response = await fetch(
      `${ENV.SUPABASE_URL}/rest/v1/api_usage?date=eq.${today}&select=*`,
      {
        headers: {
          Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
          apikey: ENV.SUPABASE_SERVICE_KEY,
        },
      }
    );

    if (!response.ok) {
      throw new Error(`Failed to fetch usage: ${response.status}`);
    }

    const records: APIUsageRecord[] = await response.json();

    // Aggregate totals
    const totalCalls = records.reduce((sum, r) => sum + r.calls_count, 0);
    const totalCost = records.reduce((sum, r) => sum + r.cost_usd, 0);
    const geminiCalls = records
      .filter((r) => r.api_name === "gemini")
      .reduce((sum, r) => sum + r.calls_count, 0);
    const geminiCost = records
      .filter((r) => r.api_name === "gemini")
      .reduce((sum, r) => sum + r.cost_usd, 0);
    const ghlCalls = records
      .filter((r) => r.api_name === "ghl")
      .reduce((sum, r) => sum + r.calls_count, 0);

    const remainingCalls = Math.max(0, COST_LIMITS.DAILY_API_CALLS_LIMIT - totalCalls);
    const remainingBudget = Math.max(0, COST_LIMITS.DAILY_COST_LIMIT_USD - totalCost);
    const warningLevel = getCostWarningLevel(totalCost, COST_LIMITS.DAILY_COST_LIMIT_USD);

    return {
      date: today,
      total_calls: totalCalls,
      total_cost_usd: totalCost,
      gemini_calls: geminiCalls,
      gemini_cost_usd: geminiCost,
      ghl_calls: ghlCalls,
      remaining_calls: remainingCalls,
      remaining_budget_usd: remainingBudget,
      warning_level: warningLevel,
      can_process_more: remainingCalls > 0 && remainingBudget > 0,
    };
  } catch (error: any) {
    logger.error("Failed to get usage summary", {
      error: error.message,
    });

    // Return safe defaults on error
    return {
      date: today,
      total_calls: 0,
      total_cost_usd: 0,
      gemini_calls: 0,
      gemini_cost_usd: 0,
      ghl_calls: 0,
      remaining_calls: COST_LIMITS.DAILY_API_CALLS_LIMIT,
      remaining_budget_usd: COST_LIMITS.DAILY_COST_LIMIT_USD,
      warning_level: "ok",
      can_process_more: true,
    };
  }
}

/**
 * Check if batch can be processed within limits
 */
export async function canProcessBatch(fileCount: number): Promise<{
  allowed: boolean;
  reason?: string;
  usage: DailyUsageSummary;
  estimate: CostEstimate;
}> {
  const usage = await getTodayUsageSummary();

  // Estimate API calls needed for batch
  // Per file: 1 classification + 1 text extraction + 1 quick parse + 1 full parse + 1 GHL create + 1 GHL update = 6 calls
  const estimatedCalls = fileCount * 6;
  const estimatedCost = fileCount * COST_LIMITS.ESTIMATED_COST_PER_CV;

  const estimate: CostEstimate = {
    estimated_calls: estimatedCalls,
    estimated_cost_usd: estimatedCost,
    would_exceed_limit: false,
    remaining_budget_usd: usage.remaining_budget_usd,
    warning_level: "ok",
  };

  // Check call limit
  if (wouldExceedAPILimit(usage.total_calls, estimatedCalls)) {
    estimate.would_exceed_limit = true;
    return {
      allowed: false,
      reason: `Daily API call limit would be exceeded. Current: ${usage.total_calls}, Estimated: +${estimatedCalls}, Limit: ${COST_LIMITS.DAILY_API_CALLS_LIMIT}`,
      usage,
      estimate,
    };
  }

  // Check cost limit
  if (wouldExceedCostLimit(usage.total_cost_usd, fileCount)) {
    estimate.would_exceed_limit = true;
    return {
      allowed: false,
      reason: `Daily cost limit would be exceeded. Current: ${formatCost(usage.total_cost_usd)}, Estimated: +${formatCost(estimatedCost)}, Limit: ${formatCost(COST_LIMITS.DAILY_COST_LIMIT_USD)}`,
      usage,
      estimate,
    };
  }

  // Calculate warning level after this batch
  const projectedCost = usage.total_cost_usd + estimatedCost;
  estimate.warning_level = getCostWarningLevel(projectedCost, COST_LIMITS.DAILY_COST_LIMIT_USD);

  return {
    allowed: true,
    usage,
    estimate,
  };
}

// ============================================
// COST CALCULATION
// ============================================

/**
 * Calculate cost for Gemini API call
 */
export function calculateGeminiCost(inputTokens: number, outputTokens: number): number {
  const inputCost = (inputTokens / 1_000_000) * GEMINI_CONFIG.COST_PER_1M_INPUT_TOKENS;
  const outputCost = (outputTokens / 1_000_000) * GEMINI_CONFIG.COST_PER_1M_OUTPUT_TOKENS;
  return inputCost + outputCost;
}

/**
 * Estimate tokens for text (rough approximation: 1 token ≈ 4 characters)
 */
export function estimateTokens(text: string): number {
  return Math.ceil(text.length / 4);
}

// ============================================
// TRACKING HELPERS
// ============================================

/**
 * Track Gemini text extraction call
 */
export async function trackTextExtraction(batchId: string, textLength: number): Promise<void> {
  const inputTokens = GEMINI_CONFIG.TOKENS_TEXT_EXTRACTION;
  const outputTokens = estimateTokens(textLength.toString());
  const cost = calculateGeminiCost(inputTokens, outputTokens);

  await recordAPIUsage({
    date: new Date().toISOString().split("T")[0],
    api_name: "gemini",
    operation_type: "text_extraction",
    calls_count: 1,
    tokens_used: inputTokens + outputTokens,
    cost_usd: cost,
    batch_id: batchId,
  });

  logger.info("Tracked text extraction", {
    batchId,
    tokens: inputTokens + outputTokens,
    cost: formatCost(cost),
  });
}

/**
 * Track Gemini quick parse call
 */
export async function trackQuickParse(batchId: string): Promise<void> {
  const inputTokens = GEMINI_CONFIG.TOKENS_QUICK_PARSE;
  const outputTokens = 100; // Small JSON output
  const cost = calculateGeminiCost(inputTokens, outputTokens);

  await recordAPIUsage({
    date: new Date().toISOString().split("T")[0],
    api_name: "gemini",
    operation_type: "quick_parse",
    calls_count: 1,
    tokens_used: inputTokens + outputTokens,
    cost_usd: cost,
    batch_id: batchId,
  });
}

/**
 * Track Gemini full parse call
 */
export async function trackFullParse(batchId: string): Promise<void> {
  const inputTokens = GEMINI_CONFIG.TOKENS_FULL_PARSE;
  const outputTokens = 500; // Large JSON output
  const cost = calculateGeminiCost(inputTokens, outputTokens);

  await recordAPIUsage({
    date: new Date().toISOString().split("T")[0],
    api_name: "gemini",
    operation_type: "full_parse",
    calls_count: 1,
    tokens_used: inputTokens + outputTokens,
    cost_usd: cost,
    batch_id: batchId,
  });
}

/**
 * Track Gemini classification call
 */
export async function trackClassification(batchId: string): Promise<void> {
  const inputTokens = GEMINI_CONFIG.TOKENS_CLASSIFICATION;
  const outputTokens = 50; // Small JSON output
  const cost = calculateGeminiCost(inputTokens, outputTokens);

  await recordAPIUsage({
    date: new Date().toISOString().split("T")[0],
    api_name: "gemini",
    operation_type: "document_classification",
    calls_count: 1,
    tokens_used: inputTokens + outputTokens,
    cost_usd: cost,
    batch_id: batchId,
  });
}

/**
 * Track GHL API call
 */
export async function trackGHLCall(
  operation: "create_contact" | "update_contact" | "search_contact" | "upload_file",
  batchId?: string
): Promise<void> {
  await recordAPIUsage({
    date: new Date().toISOString().split("T")[0],
    api_name: "ghl",
    operation_type: operation,
    calls_count: 1,
    cost_usd: 0, // GHL has no per-call cost, but we track usage
    batch_id: batchId,
  });
}

// ============================================
// REPORTING
// ============================================

/**
 * Log daily usage summary
 */
export async function logDailyUsageSummary(): Promise<void> {
  const usage = await getTodayUsageSummary();

  logger.info("📊 Daily API Usage Summary", {
    date: usage.date,
    total_calls: usage.total_calls,
    total_cost: formatCost(usage.total_cost_usd),
    gemini_calls: usage.gemini_calls,
    gemini_cost: formatCost(usage.gemini_cost_usd),
    ghl_calls: usage.ghl_calls,
    remaining_calls: usage.remaining_calls,
    remaining_budget: formatCost(usage.remaining_budget_usd),
    warning_level: usage.warning_level,
    percentage_used: `${((usage.total_cost_usd / COST_LIMITS.DAILY_COST_LIMIT_USD) * 100).toFixed(1)}%`,
  });

  // Warn if approaching limits
  if (usage.warning_level === "critical") {
    logger.warn("⚠️ CRITICAL: Approaching daily cost limit!", {
      current: formatCost(usage.total_cost_usd),
      limit: formatCost(COST_LIMITS.DAILY_COST_LIMIT_USD),
      remaining: formatCost(usage.remaining_budget_usd),
    });
  } else if (usage.warning_level === "exceeded") {
    logger.error("🚨 EXCEEDED: Daily cost limit exceeded!", {
      current: formatCost(usage.total_cost_usd),
      limit: formatCost(COST_LIMITS.DAILY_COST_LIMIT_USD),
      overage: formatCost(usage.total_cost_usd - COST_LIMITS.DAILY_COST_LIMIT_USD),
    });
  }
}

/**
 * Get batch cost summary
 */
export async function getBatchCostSummary(batchId: string): Promise<{
  total_calls: number;
  total_cost_usd: number;
  by_operation: Record<string, { calls: number; cost: number }>;
}> {
  try {
    const today = new Date().toISOString().split("T")[0];
    const response = await fetch(
      `${ENV.SUPABASE_URL}/rest/v1/api_usage?date=eq.${today}&batch_id=eq.${batchId}&select=*`,
      {
        headers: {
          Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
          apikey: ENV.SUPABASE_SERVICE_KEY,
        },
      }
    );

    if (!response.ok) {
      throw new Error(`Failed to fetch batch usage: ${response.status}`);
    }

    const records: APIUsageRecord[] = await response.json();

    const byOperation: Record<string, { calls: number; cost: number }> = {};

    for (const record of records) {
      if (!byOperation[record.operation_type]) {
        byOperation[record.operation_type] = { calls: 0, cost: 0 };
      }
      byOperation[record.operation_type].calls += record.calls_count;
      byOperation[record.operation_type].cost += record.cost_usd;
    }

    return {
      total_calls: records.reduce((sum, r) => sum + r.calls_count, 0),
      total_cost_usd: records.reduce((sum, r) => sum + r.cost_usd, 0),
      by_operation: byOperation,
    };
  } catch (error: any) {
    logger.error("Failed to get batch cost summary", {
      batchId,
      error: error.message,
    });

    return {
      total_calls: 0,
      total_cost_usd: 0,
      by_operation: {},
    };
  }
}

/**
 * Log batch cost summary
 */
export async function logBatchCostSummary(batchId: string): Promise<void> {
  const summary = await getBatchCostSummary(batchId);

  logger.info("💰 Batch Cost Summary", {
    batch_id: batchId,
    total_calls: summary.total_calls,
    total_cost: formatCost(summary.total_cost_usd),
    operations: summary.by_operation,
  });
}
