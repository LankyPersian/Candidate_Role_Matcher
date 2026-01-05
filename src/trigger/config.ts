// ============================================
// HEXONA CV PROCESSOR - CONFIGURATION
// ============================================
// Centralized configuration for all system components
// Last Updated: January 4, 2026

// ============================================
// ENVIRONMENT VARIABLES (loaded from Trigger.dev)
// ============================================
export const ENV = {
  SUPABASE_URL: process.env.SUPABASE_URL || "https://nxlzdqskcqbikzpxhjam.supabase.co",
  SUPABASE_SERVICE_KEY: process.env.SUPABASE_SERVICE_KEY!,
  GEMINI_API_KEY: process.env.GEMINI_API_KEY!,
  GHL_PRIVATE_KEY: process.env.GHL_PRIVATE_INTEGRATION_KEY!,
  GHL_LOCATION_ID: process.env.GHL_LOCATION_ID!,
  CLIENT_ID: process.env.CLIENT_ID, // Optional: Set default client UUID for testing
} as const;

// ============================================
// API CONFIGURATION
// ============================================

// Gemini API Configuration
export const GEMINI_CONFIG = {
  // Model selection
  MODEL: "gemini-2.0-flash-exp", // Latest experimental model
  FALLBACK_MODEL: "gemini-2.0-flash", // Stable fallback
  
  // API endpoints
  BASE_URL: "https://generativelanguage.googleapis.com/v1beta",
  
  // Rate limiting
  REQUESTS_PER_MINUTE: 60,
  REQUESTS_PER_DAY: 1500,
  
  // Cost tracking (approximate tokens per operation)
  TOKENS_TEXT_EXTRACTION: 2000, // Average for OCR
  TOKENS_QUICK_PARSE: 1000,
  TOKENS_FULL_PARSE: 3000,
  TOKENS_CLASSIFICATION: 800,
  TOKENS_SUMMARIZATION: 500,
  
  // Cost per 1M tokens (update these based on current pricing)
  COST_PER_1M_INPUT_TOKENS: 0.15, // $0.15 per 1M input tokens
  COST_PER_1M_OUTPUT_TOKENS: 0.60, // $0.60 per 1M output tokens
  
  // Retry configuration
  MAX_RETRIES: 6,
  INITIAL_RETRY_DELAY_MS: 1200,
  MAX_RETRY_DELAY_MS: 20000,
  RETRY_JITTER_MS: 400,
  
  // Timeouts
  TIMEOUT_MS: 60000, // 60 seconds per request
  
  // Retry on these status codes
  RETRYABLE_STATUS_CODES: [429, 500, 503],
} as const;

// GoHighLevel API Configuration
export const GHL_CONFIG = {
  // API endpoints
  BASE_URL: "https://services.leadconnectorhq.com",
  API_VERSION: "2021-07-28",
  
  // Rate limiting
  REQUESTS_PER_MINUTE: 100,
  
  // Retry configuration
  MAX_RETRIES: 4,
  INITIAL_RETRY_DELAY_MS: 800,
  MAX_RETRY_DELAY_MS: 8000,
  RETRY_JITTER_MS: 250,
  
  // Timeouts
  TIMEOUT_MS: 30000, // 30 seconds
  
  // Contact creation
  DEFAULT_TAGS: ["cv-imported"],
  
  // Retry on these status codes
  RETRYABLE_STATUS_CODES: [429, 500, 503],
} as const;

// Supabase Configuration
export const SUPABASE_CONFIG = {
  // Storage
  STORAGE_BUCKET: "cv-uploads",
  
  // Rate limiting
  MAX_CONCURRENT_OPERATIONS: 10,
  
  // Retry configuration
  MAX_RETRIES: 3,
  INITIAL_RETRY_DELAY_MS: 500,
  
  // Timeouts
  TIMEOUT_MS: 30000,
} as const;

// ============================================
// COST PROTECTION
// ============================================
export const COST_LIMITS = {
  // Daily limits
  DAILY_API_CALLS_LIMIT: 1500,
  DAILY_COST_LIMIT_USD: 50.00,
  
  // Per-batch limits
  MAX_CVS_PER_BATCH: 500,
  
  // Warning thresholds (percentage of limit)
  WARNING_THRESHOLD: 0.80, // 80%
  CRITICAL_THRESHOLD: 0.95, // 95%
  
  // Cost estimation (per CV)
  ESTIMATED_COST_PER_CV: 0.033, // ~$0.033 per CV (all operations)
} as const;

// ============================================
// DOCUMENT CLASSIFICATION
// ============================================
export const CLASSIFICATION_CONFIG = {
  // Confidence thresholds
  MIN_CONFIDENCE_THRESHOLD: 0.70, // 70% confidence required
  HIGH_CONFIDENCE_THRESHOLD: 0.90, // 90% = high confidence
  
  // Document types
  VALID_DOCUMENT_TYPES: ["cv", "resume"],
  INVALID_DOCUMENT_TYPES: ["invoice", "letter", "contract", "form", "other"],
  
  // Processing decisions
  AUTO_REJECT_BELOW_THRESHOLD: true,
  HOLD_FOR_REVIEW_ON_UNCERTAINTY: false, // Auto-reject instead
  
  // Sample size for classification
  TEXT_SAMPLE_LENGTH: 3000, // Characters to analyze
} as const;

// ============================================
// PROCESSING CONFIGURATION
// ============================================
export const PROCESSING_CONFIG = {
  // Delays between operations (to manage rate limits)
  DELAY_BETWEEN_FILES_MS: 100,
  DELAY_AFTER_GEMINI_CALL_MS: 1500,
  DELAY_AFTER_GHL_CALL_MS: 300,
  
  // Text extraction
  MAX_TEXT_LENGTH_FOR_PARSE: 8000, // Characters to send to Gemini
  MIN_TEXT_LENGTH_REQUIRED: 50, // Minimum viable CV text
  
  // Phone number validation
  MIN_PHONE_DIGITS: 10,
  MAX_PHONE_DIGITS: 15,
  
  // Duplicate detection
  ENABLE_SUPABASE_DUPLICATE_CHECK: true,
  ENABLE_GHL_DUPLICATE_CHECK: true,
  
  // Hold queue
  REQUIRE_EMAIL_OR_PHONE: true,
  AUTO_HOLD_ON_MISSING_CONTACT: true,
  AUTO_HOLD_ON_DUPLICATE: true,
  
  // Batch management
  MAX_BATCH_DURATION_MINUTES: 60,
  BATCH_STATUS_CHECK_INTERVAL_MS: 2000,
} as const;

// ============================================
// GHL FIELD MAPPING CONFIGURATION
// ============================================
export const GHL_FIELD_MAPPING = {
  // Which fields to sync (true = sync, false = skip)
  SYNC_FIELDS: {
    // Core identity (always sync)
    cv_summary: true,
    current_job_title: true,
    current_tenure: true,
    
    // Work history (formatted)
    past_work_experiences: true,
    
    // Skills & qualifications
    candidate_skills_summery: true, // Note: GHL has typo "summery"
    candidate_education_history: true,
    candidate_qualifications: true,
    professional_memberships: true,
    
    // Languages & personal
    languages_spoken: true,
    candidate_hobbies: true,
    
    // Career details
    future_job_aspirations: true,
    candidate_salary_expectation: true,
    current_notice_period: true,
    
    // Location & authorization
    nationality_nonbritish_visa_either_cu: true,
    
    // References
    references_contact_information: true,
    
    // Additional info
    linked_in_url: true,
    military_experience: true,
    
    // Raw CV text (for AI matching later)
    candidate_provided_cv_text: true,
  },
  
  // Maximum field lengths (GHL limits)
  MAX_FIELD_LENGTH: {
    text: 5000,
    textarea: 50000,
    url: 2000,
  },
} as const;

// ============================================
// LOGGING CONFIGURATION
// ============================================
export const LOGGING_CONFIG = {
  // Log levels
  ENABLE_DEBUG_LOGS: true,
  ENABLE_INFO_LOGS: true,
  ENABLE_WARN_LOGS: true,
  ENABLE_ERROR_LOGS: true,
  
  // What to log
  LOG_API_REQUESTS: true,
  LOG_API_RESPONSES: false, // Only on errors
  LOG_RETRY_ATTEMPTS: true,
  LOG_COST_TRACKING: true,
  LOG_DUPLICATE_DETECTION: true,
  
  // Log truncation
  MAX_LOG_LENGTH: 2000, // Characters
  TRUNCATE_SENSITIVE_DATA: true,
} as const;

// ============================================
// ERROR HANDLING
// ============================================
export const ERROR_CONFIG = {
  // Fail-safe behavior
  CONTINUE_ON_FILE_ERROR: true, // Don't fail entire batch
  SAVE_TO_SUPABASE_ON_GHL_FAILURE: true,
  CREATE_HOLD_QUEUE_ON_PARSE_FAILURE: false, // Reject instead
  
  // Error categorization
  RECOVERABLE_ERRORS: [
    "rate_limit",
    "network_timeout",
    "service_unavailable",
    "temporary_failure",
  ],
  
  UNRECOVERABLE_ERRORS: [
    "invalid_api_key",
    "insufficient_quota",
    "malformed_request",
    "permanent_failure",
  ],
} as const;

// ============================================
// VALIDATION RULES
// ============================================
export const VALIDATION_RULES = {
  // Email validation
  EMAIL_REGEX: /^[^\s@]+@[^\s@]+\.[^\s@]+$/,
  
  // Phone validation
  PHONE_REGEX: /(\+?\d[\d\s().-]{8,}\d)/g,
  
  // Name validation
  MIN_NAME_LENGTH: 2,
  MAX_NAME_LENGTH: 100,
  
  // File validation
  ALLOWED_EXTENSIONS: [".pdf", ".docx", ".doc", ".txt"],
  MAX_FILE_SIZE_MB: 10,
  
  // CV validation
  MIN_CV_WORDS: 50,
  MIN_CV_CHARACTERS: 200,
} as const;

// ============================================
// HELPER FUNCTIONS
// ============================================

/**
 * Calculate estimated cost for a batch
 */
export function estimateBatchCost(fileCount: number): number {
  return fileCount * COST_LIMITS.ESTIMATED_COST_PER_CV;
}

/**
 * Check if cost limit would be exceeded
 */
export function wouldExceedCostLimit(
  currentDailyCost: number,
  additionalFiles: number
): boolean {
  const estimatedAdditionalCost = estimateBatchCost(additionalFiles);
  return currentDailyCost + estimatedAdditionalCost > COST_LIMITS.DAILY_COST_LIMIT_USD;
}

/**
 * Get retry delay with exponential backoff and jitter
 */
export function getRetryDelay(
  attempt: number,
  config: { INITIAL_RETRY_DELAY_MS: number; MAX_RETRY_DELAY_MS: number; RETRY_JITTER_MS: number }
): number {
  const baseMs = config.INITIAL_RETRY_DELAY_MS;
  const backoffMs = Math.round(baseMs * Math.pow(2, attempt - 1));
  const jitterMs = Math.floor(Math.random() * config.RETRY_JITTER_MS);
  return Math.min(config.MAX_RETRY_DELAY_MS, backoffMs + jitterMs);
}

/**
 * Check if status code is retryable
 */
export function isRetryableStatus(status: number, retryableCodes: readonly number[]): boolean {
  return retryableCodes.includes(status);
}

/**
 * Truncate sensitive data for logging
 */
export function truncateForLog(text: string, maxLength: number = LOGGING_CONFIG.MAX_LOG_LENGTH): string {
  if (!text || text.length <= maxLength) return text;
  return text.substring(0, maxLength) + "...[truncated]";
}

/**
 * Validate email format
 */
export function isValidEmail(email: string | null | undefined): boolean {
  if (!email) return false;
  return VALIDATION_RULES.EMAIL_REGEX.test(email.trim());
}

/**
 * Normalize phone number for storage/comparison
 */
export function normalizePhone(phone: string | null | undefined): string | null {
  if (!phone) return null;
  
  // Remove all non-digit characters except leading +
  let cleaned = phone.trim().replace(/[\s\-\(\)\.]/g, "");
  
  // Remove leading +
  cleaned = cleaned.replace(/^\+/, "");
  
  // Remove (0) pattern
  cleaned = cleaned.replace(/\(0\)/, "");
  
  // Convert UK format (0XXXXXXXXXX) to international (44XXXXXXXXXX)
  if (cleaned.startsWith("0") && cleaned.length === 11) {
    cleaned = "44" + cleaned.substring(1);
  }
  
  // Validate length
  const digitCount = cleaned.replace(/\D/g, "").length;
  if (digitCount < VALIDATION_RULES.MIN_PHONE_LENGTH || digitCount > VALIDATION_RULES.MAX_PHONE_LENGTH) {
    return null;
  }
  
  return cleaned;
}

// Add missing validation rules
Object.assign(VALIDATION_RULES, {
  MIN_PHONE_LENGTH: 10,
  MAX_PHONE_LENGTH: 15,
});

/**
 * Generate unique batch ID
 */
export function generateBatchId(): string {
  return `batch_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
}

/**
 * Calculate processing timeout for batch
 */
export function calculateBatchTimeout(fileCount: number): number {
  // Estimate: 10 seconds per file + 5 minute buffer
  return Math.min(
    fileCount * 10000 + 300000,
    PROCESSING_CONFIG.MAX_BATCH_DURATION_MINUTES * 60000
  );
}

/**
 * Check if API call limit would be exceeded
 */
export function wouldExceedAPILimit(
  currentDailyCalls: number,
  additionalCalls: number
): boolean {
  return currentDailyCalls + additionalCalls > COST_LIMITS.DAILY_API_CALLS_LIMIT;
}

/**
 * Format cost for display
 */
export function formatCost(costUSD: number): string {
  return `$${costUSD.toFixed(4)}`;
}

/**
 * Get cost warning level
 */
export function getCostWarningLevel(
  currentCost: number,
  limit: number
): "ok" | "warning" | "critical" | "exceeded" {
  const percentage = currentCost / limit;
  
  if (percentage >= 1.0) return "exceeded";
  if (percentage >= COST_LIMITS.CRITICAL_THRESHOLD) return "critical";
  if (percentage >= COST_LIMITS.WARNING_THRESHOLD) return "warning";
  return "ok";
}

// ============================================
// TYPE EXPORTS
// ============================================
export type CostWarningLevel = "ok" | "warning" | "critical" | "exceeded";
export type DocumentType = "cv" | "resume" | "invoice" | "letter" | "contract" | "form" | "other";
export type ProcessingStatus = "pending" | "processing" | "complete" | "failed" | "awaiting_input";
export type CandidateStatus = "pending_ghl_sync" | "complete" | "ghl_sync_failed";
export type HoldQueueStatus = "pending" | "ready_for_processing" | "complete" | "skipped";
