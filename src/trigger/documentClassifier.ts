// ============================================
// DOCUMENT CLASSIFIER
// ============================================
// AI-powered document type detection to prevent processing
// of invoices, letters, and other non-CV documents

import { logger } from "@trigger.dev/sdk";
import {
  ENV,
  CLASSIFICATION_CONFIG,
  GEMINI_CONFIG,
  getRetryDelay,
  isRetryableStatus,
  truncateForLog,
} from "./config";
import { trackClassification } from "./costTracker";

// ============================================
// TYPES
// ============================================

export interface ClassificationResult {
  document_type: "cv" | "resume" | "invoice" | "letter" | "contract" | "form" | "other";
  confidence: number; // 0.0 to 1.0
  reasoning: string;
  should_process: boolean;
  key_indicators: string[];
  rejection_reason?: string;
}

// ============================================
// HELPER: DELAY
// ============================================

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ============================================
// GEMINI API CALL WITH RETRY
// ============================================

async function geminiGenerateText(args: {
  parts: any[];
  responseMimeType?: string;
  temperature?: number;
}): Promise<string> {
  const url = `${GEMINI_CONFIG.BASE_URL}/models/${GEMINI_CONFIG.MODEL}:generateContent?key=${ENV.GEMINI_API_KEY}`;
  const maxAttempts = GEMINI_CONFIG.MAX_RETRIES;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          contents: [{ parts: args.parts }],
          generationConfig: {
            temperature: args.temperature ?? 0,
            ...(args.responseMimeType ? { responseMimeType: args.responseMimeType } : {}),
          },
        }),
      });

      const bodyText = await response.text();

      if (response.ok) {
        let data: any;
        try {
          data = JSON.parse(bodyText);
        } catch {
          throw new Error("Gemini returned non-JSON envelope");
        }

        const text = data?.candidates?.[0]?.content?.parts?.[0]?.text ?? "";
        return typeof text === "string" ? text : "";
      }

      const status = response.status;
      const retryable = isRetryableStatus(status, GEMINI_CONFIG.RETRYABLE_STATUS_CODES);

      logger.warn("Gemini classification request failed", {
        attempt,
        status,
        retryable,
        bodyPreview: bodyText.slice(0, 500),
      });

      if (!retryable || attempt === maxAttempts) {
        throw new Error(`Gemini request failed (status ${status}): ${bodyText.slice(0, 500)}`);
      }

      const waitMs = getRetryDelay(attempt, GEMINI_CONFIG);
      logger.info("Retrying Gemini classification", { attempt, waitMs });
      await delay(waitMs);
    } catch (error: any) {
      if (attempt === maxAttempts) {
        throw error;
      }
      logger.warn("Gemini classification exception", {
        attempt,
        error: error.message,
      });
      const waitMs = getRetryDelay(attempt, GEMINI_CONFIG);
      await delay(waitMs);
    }
  }

  throw new Error("Gemini request failed after retries");
}

// ============================================
// JSON CLEANING
// ============================================

function stripJsonFences(s: string): string {
  let t = (s || "").trim();
  if (t.startsWith("```")) {
    t = t.replace(/^```[a-zA-Z]*\n?/, "").replace(/```$/, "").trim();
  }
  return t;
}

// ============================================
// CLASSIFICATION
// ============================================

/**
 * Classify document type using AI
 */
export async function classifyDocument(
  rawText: string,
  fileName: string,
  batchId?: string
): Promise<ClassificationResult> {
  // Validation
  if (!rawText || rawText.trim().length < 50) {
    return {
      document_type: "other",
      confidence: 1.0,
      reasoning: "Document has insufficient text content",
      should_process: false,
      key_indicators: ["empty_or_too_short"],
      rejection_reason: "Document text is too short (< 50 characters)",
    };
  }

  // Sample text for classification (reduce cost)
  const sampleText = rawText.substring(0, CLASSIFICATION_CONFIG.TEXT_SAMPLE_LENGTH);

  try {
    logger.info("Starting document classification", {
      fileName,
      textLength: rawText.length,
      sampleLength: sampleText.length,
    });

    const prompt = `Analyze this document and classify its type. Return ONLY valid JSON in this exact format:

{
  "document_type": "cv" | "resume" | "invoice" | "letter" | "contract" | "form" | "other",
  "confidence": 0.95,
  "reasoning": "Brief explanation of why this classification was chosen",
  "key_indicators": ["list", "of", "key", "indicators", "found"]
}

Classification guidelines:
- "cv" or "resume": Contains work history, education, skills, professional experience
- "invoice": Contains billing items, amounts, invoice numbers, payment terms
- "letter": Formal correspondence, cover letter, recommendation letter
- "contract": Legal terms, agreement clauses, signatures required
- "form": Application form, survey, questionnaire
- "other": None of the above

Document text (first ${sampleText.length} characters):
${sampleText}

Return ONLY the JSON object, no other text.`;

    const output = await geminiGenerateText({
      parts: [{ text: prompt }],
      responseMimeType: "application/json",
      temperature: 0,
    });

    // Track cost
    if (batchId) {
      await trackClassification(batchId);
    }

    const cleaned = stripJsonFences(output);

    let classification: any;
    try {
      classification = JSON.parse(cleaned);
    } catch (parseError) {
      logger.error("Failed to parse Gemini classification output", {
        fileName,
        outputPreview: truncateForLog(cleaned, 500),
      });

      // Fallback: try to extract type from text
      const lowerOutput = output.toLowerCase();
      if (lowerOutput.includes('"cv"') || lowerOutput.includes('"resume"')) {
        return {
          document_type: "cv",
          confidence: 0.6,
          reasoning: "Classification parsing failed, but detected CV indicators",
          should_process: true,
          key_indicators: ["fallback_detection"],
        };
      }

      // If we can't classify, reject to be safe
      return {
        document_type: "other",
        confidence: 0.5,
        reasoning: "Classification failed - unable to parse AI response",
        should_process: false,
        key_indicators: ["classification_failed"],
        rejection_reason: "Unable to determine document type",
      };
    }

    // Validate classification structure
    if (!classification.document_type || typeof classification.confidence !== "number") {
      logger.error("Invalid classification structure", {
        fileName,
        classification,
      });

      return {
        document_type: "other",
        confidence: 0.5,
        reasoning: "Classification returned invalid structure",
        should_process: false,
        key_indicators: ["invalid_classification"],
        rejection_reason: "Invalid classification response",
      };
    }

    // Normalize confidence to 0-1 range
    const confidence = Math.min(1.0, Math.max(0.0, classification.confidence));

    // Determine if should process
    const isValidType =
      classification.document_type === "cv" || classification.document_type === "resume";
    const meetsConfidenceThreshold = confidence >= CLASSIFICATION_CONFIG.MIN_CONFIDENCE_THRESHOLD;
    const shouldProcess = isValidType && meetsConfidenceThreshold;

    // Build result
    const result: ClassificationResult = {
      document_type: classification.document_type,
      confidence,
      reasoning: classification.reasoning || "No reasoning provided",
      should_process: shouldProcess,
      key_indicators: Array.isArray(classification.key_indicators)
        ? classification.key_indicators
        : [],
    };

    // Add rejection reason if not processing
    if (!shouldProcess) {
      if (!isValidType) {
        result.rejection_reason = `Document identified as '${classification.document_type}', not a CV/resume`;
      } else {
        result.rejection_reason = `Confidence too low: ${(confidence * 100).toFixed(1)}% (required: ${(CLASSIFICATION_CONFIG.MIN_CONFIDENCE_THRESHOLD * 100).toFixed(0)}%)`;
      }
    }

    logger.info("Document classification complete", {
      fileName,
      type: result.document_type,
      confidence: `${(result.confidence * 100).toFixed(1)}%`,
      shouldProcess: result.should_process,
      reasoning: result.reasoning,
    });

    return result;
  } catch (error: any) {
    logger.error("Document classification failed", {
      fileName,
      error: error.message,
    });

    // Fail-safe: reject on error to prevent processing non-CVs
    return {
      document_type: "other",
      confidence: 0.0,
      reasoning: `Classification failed with error: ${error.message}`,
      should_process: false,
      key_indicators: ["classification_error"],
      rejection_reason: `Classification system error: ${error.message}`,
    };
  }
}

/**
 * Batch classify multiple documents
 * Returns array of results in same order as inputs
 */
export async function classifyDocuments(
  documents: Array<{ rawText: string; fileName: string }>,
  batchId?: string
): Promise<ClassificationResult[]> {
  const results: ClassificationResult[] = [];

  for (let i = 0; i < documents.length; i++) {
    const doc = documents[i];

    try {
      const result = await classifyDocument(doc.rawText, doc.fileName, batchId);
      results.push(result);

      // Small delay between classifications to manage rate limits
      if (i < documents.length - 1) {
        await delay(500);
      }
    } catch (error: any) {
      logger.error("Batch classification failed for document", {
        index: i,
        fileName: doc.fileName,
        error: error.message,
      });

      // Add error result
      results.push({
        document_type: "other",
        confidence: 0.0,
        reasoning: `Batch classification error: ${error.message}`,
        should_process: false,
        key_indicators: ["batch_error"],
        rejection_reason: error.message,
      });
    }
  }

  return results;
}

/**
 * Get classification statistics for a batch
 */
export function getClassificationStats(results: ClassificationResult[]): {
  total: number;
  accepted: number;
  rejected: number;
  by_type: Record<string, number>;
  average_confidence: number;
  high_confidence: number;
  low_confidence: number;
} {
  const total = results.length;
  const accepted = results.filter((r) => r.should_process).length;
  const rejected = total - accepted;

  const byType: Record<string, number> = {};
  let totalConfidence = 0;
  let highConfidence = 0;
  let lowConfidence = 0;

  for (const result of results) {
    // Count by type
    byType[result.document_type] = (byType[result.document_type] || 0) + 1;

    // Sum confidence
    totalConfidence += result.confidence;

    // Count confidence levels
    if (result.confidence >= CLASSIFICATION_CONFIG.HIGH_CONFIDENCE_THRESHOLD) {
      highConfidence++;
    } else if (result.confidence < CLASSIFICATION_CONFIG.MIN_CONFIDENCE_THRESHOLD) {
      lowConfidence++;
    }
  }

  return {
    total,
    accepted,
    rejected,
    by_type: byType,
    average_confidence: total > 0 ? totalConfidence / total : 0,
    high_confidence: highConfidence,
    low_confidence: lowConfidence,
  };
}

/**
 * Log classification statistics
 */
export function logClassificationStats(
  results: ClassificationResult[],
  batchId?: string
): void {
  const stats = getClassificationStats(results);

  logger.info("📋 Document Classification Statistics", {
    batch_id: batchId,
    total: stats.total,
    accepted: stats.accepted,
    rejected: stats.rejected,
    acceptance_rate: `${((stats.accepted / stats.total) * 100).toFixed(1)}%`,
    by_type: stats.by_type,
    average_confidence: `${(stats.average_confidence * 100).toFixed(1)}%`,
    high_confidence: stats.high_confidence,
    low_confidence: stats.low_confidence,
  });

  if (stats.rejected > 0) {
    logger.warn("⚠️ Documents rejected", {
      rejected: stats.rejected,
      rejection_rate: `${((stats.rejected / stats.total) * 100).toFixed(1)}%`,
    });
  }
}

/**
 * Quick heuristic check before AI classification (cost optimization)
 * Returns true if document looks like it might be a CV (worth AI classifying)
 */
export function quickHeuristicCheck(rawText: string, fileName: string): {
  likely_cv: boolean;
  confidence: number;
  reason: string;
} {
  const lower = rawText.toLowerCase();
  const fileNameLower = fileName.toLowerCase();

  // CV-positive indicators
  const cvIndicators = [
    "curriculum vitae",
    "resume",
    "work experience",
    "employment history",
    "education",
    "qualifications",
    "skills",
    "professional summary",
    "career objective",
    "references available",
  ];

  // CV-negative indicators (strong signals it's NOT a CV)
  const negativeIndicators = [
    "invoice number",
    "payment due",
    "total amount",
    "billing address",
    "tax id",
    "purchase order",
    "quotation",
    "estimate",
    "terms and conditions",
    "contract agreement",
  ];

  // Check filename
  if (fileNameLower.includes("cv") || fileNameLower.includes("resume")) {
    return {
      likely_cv: true,
      confidence: 0.8,
      reason: "Filename contains CV/resume",
    };
  }

  if (
    fileNameLower.includes("invoice") ||
    fileNameLower.includes("bill") ||
    fileNameLower.includes("receipt")
  ) {
    return {
      likely_cv: false,
      confidence: 0.9,
      reason: "Filename indicates invoice/bill",
    };
  }

  // Count indicators in text
  let cvScore = 0;
  let negativeScore = 0;

  for (const indicator of cvIndicators) {
    if (lower.includes(indicator)) cvScore++;
  }

  for (const indicator of negativeIndicators) {
    if (lower.includes(indicator)) negativeScore++;
  }

  // Decision logic
  if (negativeScore >= 2) {
    return {
      likely_cv: false,
      confidence: 0.85,
      reason: `Found ${negativeScore} negative indicators (invoice/contract/etc)`,
    };
  }

  if (cvScore >= 3) {
    return {
      likely_cv: true,
      confidence: 0.75,
      reason: `Found ${cvScore} CV indicators`,
    };
  }

  if (cvScore >= 1) {
    return {
      likely_cv: true,
      confidence: 0.6,
      reason: `Found some CV indicators (${cvScore}), worth AI check`,
    };
  }

  // Default: uncertain, let AI decide
  return {
    likely_cv: true,
    confidence: 0.5,
    reason: "Uncertain - needs AI classification",
  };
}
