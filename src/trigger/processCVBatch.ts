// ============================================
// PROCESS CV BATCH - PRODUCTION VERSION
// ============================================
// Complete CV processing pipeline with:
// - Document classification (reject non-CVs)
// - Cost tracking and limits
// - Comprehensive error handling
// - Full 51-field GHL sync
// - Duplicate detection (Supabase + GHL)
// - Hold queue management
// - Batch recovery

import { task, logger } from "@trigger.dev/sdk";
import { Buffer } from "buffer";
import {
  ENV,
  PROCESSING_CONFIG,
  SUPABASE_CONFIG,
  GEMINI_CONFIG,
  GHL_CONFIG,
  getRetryDelay,
  isRetryableStatus,
  isValidEmail,
  normalizePhone,
  truncateForLog,
} from "./config";
import {
  canProcessBatch,
  trackTextExtraction,
  trackQuickParse,
  trackFullParse,
  trackGHLCall,
  logDailyUsageSummary,
  logBatchCostSummary,
} from "./costTracker";
import {
  classifyDocument,
  logClassificationStats,
  quickHeuristicCheck,
  ClassificationResult,
} from "./documentClassifier";
import { buildCompleteGHLCustomFields, splitName } from "./ghlTransformers";

// ============================================
// TYPES
// ============================================

interface CVBatchPayload {
  batchId: string;
  clientId: string;
}

interface ParsedCV {
  full_name: string | null;
  email: string | null;
  phone: string | null;
  address: string | null;
  linkedin_url: string | null;
  date_of_birth: string | null;
  nationality: string | null;
  visa_work_permit: string | null;
  professional_summary: string | null;
  future_job_aspirations: string | null;
  work_history: any[];
  education: any[];
  skills: string[];
  certifications: any[];
  driving_licence: string | null;
  languages: any[];
  training_courses: any[];
  professional_memberships: any[];
  awards_honours: any[];
  volunteering: any[];
  interests_hobbies: any[];
  candidate_references: any[];
  military_service: string | null;
  salary_expectation: string | null;
  notice_period: string | null;
  availability_start_date: string | null;
  relocation_willingness: string | null;
  remote_work_preference: string | null;
  cv_summary: string | null;
}

interface HoldQueueDuplicateDetails {
  full_name: string;
  email: string | null;
  phone: string | null;
  updated_at: string;
}

interface BatchStats {
  total_files: number;
  classified: number;
  rejected_by_classification: number;
  processed: number;
  failed: number;
  held_for_review: number;
  duplicates_found: number;
}

// ============================================
// HELPER: DELAY
// ============================================

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ============================================
// MAIN TASK
// ============================================

export const processCVBatch = task({
  id: "process-cv-batch",
  maxDuration: 600, // 10 minutes
  run: async (payload: CVBatchPayload) => {
    const { batchId, clientId } = payload;

    logger.info("🚀 Starting CV batch processing", { batchId, clientId });

    // Log current daily usage
    await logDailyUsageSummary();

    try {
      // 1) List all files
      const files = await listBatchFiles(batchId, clientId);
      logger.info(`📁 Found ${files.length} files to process`, { batchId });

      if (files.length === 0) {
        logger.warn("No files found in batch", { batchId, clientId });
        await updateBatchStatus(batchId, "complete", 0);
        return { processed: 0, failed: 0, held: 0, rejected: 0, batchId };
      }

      // 2) Check if batch can be processed within cost limits
      const costCheck = await canProcessBatch(files.length);
      if (!costCheck.allowed) {
        logger.error("❌ Batch rejected - cost limit exceeded", {
          batchId,
          reason: costCheck.reason,
          current_usage: costCheck.usage,
          estimate: costCheck.estimate,
        });

        await updateBatchStatus(batchId, "failed", 0);
        throw new Error(`Cost limit check failed: ${costCheck.reason}`);
      }

      logger.info("✅ Cost check passed", {
        files: files.length,
        estimated_cost: `$${costCheck.estimate.estimated_cost_usd.toFixed(4)}`,
        estimated_calls: costCheck.estimate.estimated_calls,
        remaining_budget: `$${costCheck.usage.remaining_budget_usd.toFixed(2)}`,
      });

      // 3) Mark batch as processing
      await updateBatchStatus(batchId, "processing");

      // Initialize stats
      const stats: BatchStats = {
        total_files: files.length,
        classified: 0,
        rejected_by_classification: 0,
        processed: 0,
        failed: 0,
        held_for_review: 0,
        duplicates_found: 0,
      };

      const classificationResults: ClassificationResult[] = [];

      // Use GHL token directly
      const ghlAccessToken = ENV.GHL_PRIVATE_KEY;

      // 4) Process each file sequentially
      for (let i = 0; i < files.length; i++) {
        const file = files[i];

        try {
          logger.info(`📄 Processing file ${i + 1}/${files.length}`, {
            name: file.name,
            path: file.path,
          });

          // Download file
          const fileBuffer = await downloadFile(file.path);

          // Extract text
          const rawText = await extractText(fileBuffer, file.name);
          await trackTextExtraction(batchId, rawText?.length || 0);

          logger.info("Extracted text stats", {
            file: file.name,
            length: rawText?.length ?? 0,
            preview: (rawText || "").slice(0, 200),
          });

          // Validate text length
          if (!rawText || rawText.trim().length < PROCESSING_CONFIG.MIN_TEXT_LENGTH_REQUIRED) {
            logger.warn("❌ File has insufficient text - rejecting", { file: file.name });
            stats.failed++;
            continue;
          }

          // 🔥 DOCUMENT CLASSIFICATION (cost optimization + security)
          logger.info("🔍 Classifying document", { file: file.name });

          // Quick heuristic check first (free)
          const heuristic = quickHeuristicCheck(rawText, file.name);
          logger.info("Heuristic check result", {
            file: file.name,
            likely_cv: heuristic.likely_cv,
            confidence: `${(heuristic.confidence * 100).toFixed(1)}%`,
            reason: heuristic.reason,
          });

          let classification: ClassificationResult;

          // Only run AI classification if heuristic suggests it might be a CV
          if (heuristic.likely_cv) {
            classification = await classifyDocument(rawText, file.name, batchId);
          } else {
            // Heuristic strongly indicates non-CV, save AI cost
            classification = {
              document_type: "other",
              confidence: heuristic.confidence,
              reasoning: `Heuristic pre-filter: ${heuristic.reason}`,
              should_process: false,
              key_indicators: ["heuristic_rejection"],
              rejection_reason: heuristic.reason,
            };
          }

          stats.classified++;
          classificationResults.push(classification);

          // Reject if not a CV
          if (!classification.should_process) {
            logger.warn("❌ Document rejected by classification", {
              file: file.name,
              type: classification.document_type,
              confidence: `${(classification.confidence * 100).toFixed(1)}%`,
              reason: classification.rejection_reason,
            });

            stats.rejected_by_classification++;

            // Optionally log rejection to database for tracking
            await recordRejection(batchId, file.name, file.path, classification);

            continue;
          }

          logger.info("✅ Document classified as CV - processing", {
            file: file.name,
            confidence: `${(classification.confidence * 100).toFixed(1)}%`,
          });

          // Quick parse for contact info
          const contactInfo = await quickParse(rawText);
          await trackQuickParse(batchId);

          // Check for missing contact info
          if (!contactInfo.email && !contactInfo.phone) {
            logger.warn("⚠️ Missing contact info -> hold_queue", {
              file: file.name,
              extracted: contactInfo,
            });

            await addSingleToHoldQueue(batchId, clientId, {
              file_name: file.name,
              file_path: file.path,
              extracted_name: contactInfo.full_name,
              raw_text: rawText,
              extracted_data: {
                full_name: contactInfo.full_name,
                email: contactInfo.email,
                phone: contactInfo.phone,
                reason: "missing_contact_info",
                classification: classification,
              },
            });

            stats.held_for_review++;
            continue;
          }

          // Full parse
          const parsedData = await fullParse(rawText);
          await trackFullParse(batchId);

          // Merge contact info (prefer fullParse, fallback to quickParse)
          parsedData.full_name = parsedData.full_name ?? contactInfo.full_name;
          parsedData.email = parsedData.email ?? contactInfo.email;
          parsedData.phone = parsedData.phone ?? contactInfo.phone;

          // 🔥 CHECK GHL FOR DUPLICATES
          let existingGHLContactId: string | null = null;
          try {
            existingGHLContactId = await findExistingGHLContact(
              parsedData.email,
              parsedData.phone,
              ghlAccessToken
            );
            await trackGHLCall("search_contact", batchId);
          } catch (err: any) {
            logger.warn("GHL duplicate check failed", {
              file: file.name,
              error: err?.message ?? String(err),
            });
          }

          // Fetch GHL contact details if duplicate found
          let ghlContactDetails: HoldQueueDuplicateDetails | null = null;
          if (existingGHLContactId) {
            ghlContactDetails = await fetchGHLContactDetails(existingGHLContactId, ghlAccessToken, {
              fallbackEmail: parsedData.email,
              fallbackPhone: parsedData.phone,
            });
            await trackGHLCall("search_contact", batchId);
          }

          // If duplicate found, add to hold queue
          if (existingGHLContactId) {
            logger.warn("⚠️ GHL duplicate detected -> hold_queue", {
              file: file.name,
              ghlContactId: existingGHLContactId,
              email: parsedData.email,
              phone: parsedData.phone,
            });

            await addSingleToHoldQueue(batchId, clientId, {
              file_name: file.name,
              file_path: file.path,
              extracted_name: parsedData.full_name,
              raw_text: rawText,
              extracted_data: {
                full_name: parsedData.full_name,
                email: parsedData.email,
                phone: parsedData.phone,
                ghl_duplicate_contact_id: existingGHLContactId,
                ghl_duplicate_contact_details: ghlContactDetails,
                reason: "ghl_duplicate_detected",
                classification: classification,
              },
            });

            stats.held_for_review++;
            stats.duplicates_found++;
            continue;
          }

          // No duplicate - proceed with normal processing

          // SAVE TO SUPABASE FIRST (ensures data is never lost)
          const candidateId = await writeCandidate({
            ...parsedData,
            client_id: clientId || null,
            ghl_contact_id: null,
            cv_file_path: file.path,
            cv_raw_text: rawText,
            batch_id: batchId,
            status: "pending_ghl_sync",
          });

          logger.info("✅ Candidate saved to Supabase", {
            file: file.name,
            candidateId,
          });

          // Create GHL contact and sync
          let ghlContactId: string | null = null;

          try {
            // Create contact
            ghlContactId = await createGHLContact(parsedData, ghlAccessToken);
            await trackGHLCall("create_contact", batchId);

            logger.info("✅ GHL contact created", {
              file: file.name,
              ghlContactId,
            });

            // Update with all custom fields
            await updateGHLContact(ghlContactId, parsedData, ghlAccessToken);
            await trackGHLCall("update_contact", batchId);

            logger.info("✅ GHL custom fields updated", {
              file: file.name,
              ghlContactId,
            });

            // Upload CV file
            const uploadResult = await uploadCVToGHL(
              ghlContactId,
              file.path,
              file.name,
              ghlAccessToken
            );
            await trackGHLCall("upload_file", batchId);

            if (!uploadResult.success) {
              logger.warn("⚠️ CV file upload failed", {
                file: file.name,
                contactId: ghlContactId,
                error: uploadResult.error,
              });
            } else {
              logger.info("✅ CV file uploaded to GHL", {
                file: file.name,
                ghlContactId,
              });
            }

            // Mark complete
            await updateCandidateGHL(candidateId, ghlContactId, "complete");

            logger.info("✅ GHL sync successful", {
              file: file.name,
              candidateId,
              ghlContactId,
              cvUploaded: uploadResult.success,
            });
          } catch (ghlError: any) {
            logger.error("❌ GHL sync failed", {
              file: file.name,
              candidateId,
              error: ghlError?.message ?? String(ghlError),
              stack: ghlError?.stack,
            });

            // Mark as failed
            await updateCandidateGHL(candidateId, null, "ghl_sync_failed");
          }

          stats.processed++;
          await updateBatchProgress(batchId, stats.processed, files.length);

          // Small delay between files
          if (i < files.length - 1) {
            await delay(PROCESSING_CONFIG.DELAY_BETWEEN_FILES_MS);
          }
        } catch (error: any) {
          logger.error("❌ Failed to process file", {
            file: file?.name,
            error: error?.message ?? String(error),
            stack: error?.stack,
          });
          stats.failed++;
        }
      }

      // 5) Log classification stats
      if (classificationResults.length > 0) {
        logClassificationStats(classificationResults, batchId);
      }

      // 6) Determine final batch status
      const finalStatus = stats.held_for_review > 0 ? "awaiting_input" : "complete";
      await updateBatchStatus(batchId, finalStatus, stats.processed);

      // 7) Log batch summary
      logger.info("🎉 Batch processing complete", {
        batchId,
        stats,
        final_status: finalStatus,
      });

      // 8) Log cost summary
      await logBatchCostSummary(batchId);

      return {
        processed: stats.processed,
        failed: stats.failed,
        held: stats.held_for_review,
        rejected: stats.rejected_by_classification,
        duplicates: stats.duplicates_found,
        batchId,
      };
    } catch (error: any) {
      logger.error("❌ Batch processing failed", {
        batchId,
        error: error?.message ?? String(error),
        stack: error?.stack,
      });

      // Mark batch as failed
      try {
        await updateBatchStatus(batchId, "failed", 0);
      } catch (updateError) {
        logger.error("Failed to update batch status to failed", {
          batchId,
          error: updateError,
        });
      }

      throw error;
    }
  },
});

// ============================================
// STORAGE OPERATIONS
// ============================================

function encodeStoragePath(path: string): string {
  return path.split("/").map(encodeURIComponent).join("/");
}

async function listBatchFiles(
  batchId: string,
  clientId: string
): Promise<Array<{ name: string; path: string }>> {
  function safeJsonParse(text: string): any {
    try {
      return JSON.parse(text);
    } catch {
      return null;
    }
  }

  function normalizeListResponse(parsed: any): any[] {
    if (Array.isArray(parsed)) return parsed;
    if (parsed && Array.isArray(parsed.data)) return parsed.data;
    if (parsed && Array.isArray(parsed.items)) return parsed.items;
    return [];
  }

  function toName(item: any): string | null {
    if (typeof item === "string") return item;
    if (item && typeof item.name === "string") return item.name;
    return null;
  }

  function isLikelyFileItem(item: any): boolean {
    const name = toName(item);
    if (!name || !name.trim()) return false;

    const lower = name.toLowerCase().trim();
    return (
      lower.endsWith(".pdf") ||
      lower.endsWith(".docx") ||
      lower.endsWith(".doc") ||
      lower.endsWith(".txt")
    );
  }

  async function callList(body: Record<string, any>) {
    if (!("prefix" in body)) body.prefix = "";

    const reqBody = { limit: 1000, offset: 0, ...body };

    const res = await fetch(`${ENV.SUPABASE_URL}/storage/v1/object/list/${SUPABASE_CONFIG.STORAGE_BUCKET}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
        apikey: ENV.SUPABASE_SERVICE_KEY,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(reqBody),
    });

    const text = await res.text();
    const parsed = safeJsonParse(text);

    if (!res.ok) {
      logger.error("Supabase Storage list failed", {
        status: res.status,
        requestBody: reqBody,
        bodyPreview: text.slice(0, 1200),
      });
      throw new Error(`Supabase Storage list failed (status ${res.status})`);
    }

    return {
      items: normalizeListResponse(parsed),
    };
  }

  let effectiveClientId = (clientId || "").trim();

  // Try various path patterns
  const candidatePrefixes: string[] = [];

  if (effectiveClientId) {
    candidatePrefixes.push(`${effectiveClientId}/${batchId}/`);
    candidatePrefixes.push(`${effectiveClientId}/${batchId}`);
  }
  candidatePrefixes.push(`${batchId}/`);
  candidatePrefixes.push(`${batchId}`);

  let chosenPrefix = "";
  let chosenItems: any[] = [];

  for (const p of candidatePrefixes) {
    const prefixToTry = p.endsWith("/") ? p : `${p}/`;
    const r = await callList({ prefix: prefixToTry });

    if (r.items.length > 0) {
      chosenPrefix = prefixToTry;
      chosenItems = r.items;
      break;
    }
  }

  const actualFiles = chosenItems.filter(isLikelyFileItem);

  logger.info("Storage list result", {
    prefixUsed: chosenPrefix,
    totalItems: chosenItems.length,
    actualFiles: actualFiles.length,
  });

  return actualFiles.map((f: any) => {
    const rawName = toName(f) || "";
    const leafName = rawName.includes("/") ? rawName.split("/").pop()! : rawName;
    const fullPath =
      chosenPrefix && rawName.startsWith(chosenPrefix)
        ? rawName
        : chosenPrefix
        ? `${chosenPrefix}${leafName}`
        : rawName;

    return { name: leafName, path: fullPath };
  });
}

async function downloadFile(filePath: string): Promise<ArrayBuffer> {
  const encodedPath = encodeStoragePath(filePath);

  const response = await fetch(
    `${ENV.SUPABASE_URL}/storage/v1/object/${SUPABASE_CONFIG.STORAGE_BUCKET}/${encodedPath}`,
    {
      headers: {
        Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
        apikey: ENV.SUPABASE_SERVICE_KEY,
      },
    }
  );

  if (!response.ok) {
    const err = await response.text();
    throw new Error(`Failed to download file: ${response.status} ${err.slice(0, 800)}`);
  }

  return response.arrayBuffer();
}

// Continue in next file... (character limit reached)
// ============================================
// PROCESS CV BATCH - PART 2
// ============================================
// Continuation: Gemini operations, GHL operations, database operations

// Import from part 1 (this will be merged into single file during deployment)
// For now, this shows the remaining functions

// ============================================
// GEMINI OPERATIONS
// ============================================

function stripJsonFences(s: string): string {
  let t = (s || "").trim();
  if (t.startsWith("```")) {
    t = t.replace(/^```[a-zA-Z]*\n?/, "").replace(/```$/, "").trim();
  }
  return t;
}

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

      logger.warn("Gemini request failed", {
        attempt,
        status,
        retryable,
        bodyPreview: bodyText.slice(0, 500),
      });

      if (!retryable || attempt === maxAttempts) {
        throw new Error(`Gemini request failed (status ${status}): ${bodyText.slice(0, 500)}`);
      }

      const waitMs = getRetryDelay(attempt, GEMINI_CONFIG);
      logger.info("Retrying Gemini request", { attempt, waitMs });
      await delay(waitMs);
    } catch (error: any) {
      if (attempt === maxAttempts) {
        throw error;
      }
      logger.warn("Gemini exception", {
        attempt,
        error: error.message,
      });
      const waitMs = getRetryDelay(attempt, GEMINI_CONFIG);
      await delay(waitMs);
    }
  }

  throw new Error("Gemini request failed after retries");
}

function guessGeminiMimeType(extension: string | undefined): string {
  const ext = (extension || "").toLowerCase();
  if (ext === "pdf") return "application/pdf";
  if (ext === "docx") return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
  if (ext === "doc") return "application/msword";
  if (ext === "txt") return "text/plain";
  return "application/octet-stream";
}

async function extractText(buffer: ArrayBuffer, fileName: string): Promise<string> {
  const extension = fileName.split(".").pop()?.toLowerCase();

  if (extension === "txt") return new TextDecoder().decode(buffer);

  const base64 = Buffer.from(buffer).toString("base64");
  const mimeType = guessGeminiMimeType(extension);

  logger.info("Gemini extractText called", {
    fileName,
    mimeType,
    bytes: buffer.byteLength,
  });

  const result = await geminiGenerateText({
    parts: [
      { inlineData: { mimeType, data: base64 } },
      {
        text: "Extract all text content from this document. Return only the raw text, no formatting or commentary.",
      },
    ],
    temperature: 0,
  });

  await delay(PROCESSING_CONFIG.DELAY_AFTER_GEMINI_CALL_MS);
  return result;
}

function looksLikeYearRange(s: string): boolean {
  return /\b(19|20)\d{2}\s*[-–]\s*(19|20)\d{2}\b/.test(s);
}

function selectBestPhoneCandidate(matches: string[]): string | null {
  const cleaned = matches
    .map((m) => m.trim())
    .filter((m) => m.length >= 10)
    .filter((m) => !looksLikeYearRange(m))
    .map((m) => ({ raw: m, digits: m.replace(/\D/g, "") }))
    .filter((x) => x.digits.length >= PROCESSING_CONFIG.MIN_PHONE_DIGITS && x.digits.length <= PROCESSING_CONFIG.MAX_PHONE_DIGITS);

  if (cleaned.length === 0) return null;

  cleaned.sort((a, b) => {
    const score = (x: { raw: string; digits: string }) => {
      let s = 0;
      if (x.raw.includes("+")) s += 3;
      if (x.raw.includes("(") || x.raw.includes(")")) s += 2;
      if (/[\s.-]/.test(x.raw)) s += 1;
      if (x.digits.length === 11 || x.digits.length === 12) s += 1;
      return s;
    };
    return score(b) - score(a);
  });

  return cleaned[0].raw.replace(/\s+/g, " ").trim();
}

async function quickParse(rawText: string): Promise<{
  full_name: string | null;
  email: string | null;
  phone: string | null;
}> {
  const emailMatch = rawText.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  const phoneCandidates = rawText.match(/(\+?\d[\d\s().-]{8,}\d)/g) || [];
  const phoneBest = selectBestPhoneCandidate(phoneCandidates);

  const regexResult = {
    full_name: null,
    email: emailMatch ? emailMatch[0] : null,
    phone: phoneBest ? phoneBest : null,
  };

  logger.info("quickParse regex results", regexResult);

  if (regexResult.email || regexResult.phone) {
    await delay(300);
    return regexResult;
  }

  const output = await geminiGenerateText({
    parts: [
      {
        text: `Extract ONLY the following from this CV text. Return valid JSON only, no other text:
{
  "full_name": "string or null",
  "email": "string or null",
  "phone": "string or null"
}

CV Text:
${rawText.substring(0, 3000)}`,
      },
    ],
    responseMimeType: "application/json",
    temperature: 0,
  });

  await delay(PROCESSING_CONFIG.DELAY_AFTER_GEMINI_CALL_MS);

  const cleaned = stripJsonFences(output);

  try {
    const parsed = JSON.parse(cleaned);
    return {
      full_name: parsed.full_name ?? null,
      email: parsed.email ?? null,
      phone: parsed.phone ?? null,
    };
  } catch {
    logger.warn("quickParse: Gemini returned non-JSON", {
      preview: cleaned.slice(0, 300),
    });
    return { full_name: null, email: null, phone: null };
  }
}

async function fullParse(rawText: string): Promise<ParsedCV> {
  const output = await geminiGenerateText({
    parts: [
      {
        text: `Extract all information from this CV and return as JSON. Include these fields:
{
  "full_name": "string or null",
  "email": "string or null",
  "phone": "string or null",
  "address": "string or null",
  "linkedin_url": "string or null",
  "date_of_birth": "string or null",
  "nationality": "string or null",
  "visa_work_permit": "string or null",
  "professional_summary": "string or null",
  "future_job_aspirations": "string or null",
  "work_history": [{"job_title": "", "company_name": "", "company_location": "", "start_date": "", "end_date": "", "employment_type": "", "duties_responsibilities": "", "achievements": "", "reason_for_leaving": ""}],
  "education": [{"qualification_name": "", "institution_name": "", "start_date": "", "end_date": "", "grade_classification": "", "dissertation_thesis": "", "honours_awards": "", "extracurricular": ""}],
  "skills": ["skill1", "skill2"],
  "certifications": [{"name": "", "issuing_organisation": "", "date_obtained": "", "expiry_date": "", "certification_id": ""}],
  "driving_licence": "string or null",
  "languages": [{"language": "", "proficiency": "", "reading": "", "writing": "", "speaking": "", "certifications": ""}],
  "training_courses": [{"course_name": "", "provider": "", "date_completed": "", "duration": "", "format": "", "accreditation": ""}],
  "professional_memberships": [{"organisation_name": "", "membership_type": "", "member_since": "", "member_number": ""}],
  "awards_honours": [],
  "volunteering": [],
  "interests_hobbies": [],
  "candidate_references": [],
  "military_service": "string or null",
  "salary_expectation": "string or null",
  "notice_period": "string or null",
  "availability_start_date": "string or null",
  "relocation_willingness": "string or null",
  "remote_work_preference": "string or null",
  "cv_summary": "2-3 sentence summary of the candidate for matching purposes"
}

Return ONLY valid JSON, no other text.

CV Text:
${rawText.substring(0, PROCESSING_CONFIG.MAX_TEXT_LENGTH_FOR_PARSE)}`,
      },
    ],
    responseMimeType: "application/json",
    temperature: 0,
  });

  await delay(PROCESSING_CONFIG.DELAY_AFTER_GEMINI_CALL_MS);

  const cleaned = stripJsonFences(output);

  try {
    return JSON.parse(cleaned);
  } catch {
    logger.error("Failed to parse Gemini fullParse output", {
      preview: cleaned.slice(0, 600),
    });
    return getEmptyParsedCV();
  }
}

function getEmptyParsedCV(): ParsedCV {
  return {
    full_name: null,
    email: null,
    phone: null,
    address: null,
    linkedin_url: null,
    date_of_birth: null,
    nationality: null,
    visa_work_permit: null,
    professional_summary: null,
    future_job_aspirations: null,
    work_history: [],
    education: [],
    skills: [],
    certifications: [],
    driving_licence: null,
    languages: [],
    training_courses: [],
    professional_memberships: [],
    awards_honours: [],
    volunteering: [],
    interests_hobbies: [],
    candidate_references: [],
    military_service: null,
    salary_expectation: null,
    notice_period: null,
    availability_start_date: null,
    relocation_willingness: null,
    remote_work_preference: null,
    cv_summary: null,
  };
}

// ============================================
// GHL OPERATIONS
// ============================================

async function findExistingGHLContact(
  email: string | null,
  phone: string | null,
  accessToken: string
): Promise<string | null> {
  if (email && isValidEmail(email)) {
    const emailUrl = `${GHL_CONFIG.BASE_URL}/contacts/?locationId=${ENV.GHL_LOCATION_ID}&query=${encodeURIComponent(
      email
    )}`;

    const emailResponse = await fetch(emailUrl, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        Version: GHL_CONFIG.API_VERSION,
      },
    });

    if (emailResponse.ok) {
      const data = await emailResponse.json();
      if (data?.contacts && data.contacts.length > 0) {
        logger.info("Found existing GHL contact by email", {
          email,
          contactId: data.contacts[0].id,
        });
        return data.contacts[0].id;
      }
    }
  }

  if (phone) {
    const normalized = normalizePhone(phone);
    if (normalized) {
      const phoneUrl = `${GHL_CONFIG.BASE_URL}/contacts/?locationId=${ENV.GHL_LOCATION_ID}&query=${encodeURIComponent(
        normalized
      )}`;

      const phoneResponse = await fetch(phoneUrl, {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          Version: GHL_CONFIG.API_VERSION,
        },
      });

      if (phoneResponse.ok) {
        const data = await phoneResponse.json();
        if (data?.contacts && data.contacts.length > 0) {
          logger.info("Found existing GHL contact by phone", {
            phone: normalized,
            contactId: data.contacts[0].id,
          });
          return data.contacts[0].id;
        }
      }
    }
  }

  return null;
}

async function fetchGHLContactDetails(
  contactId: string,
  accessToken: string,
  opts: { fallbackEmail: string | null; fallbackPhone: string | null }
): Promise<HoldQueueDuplicateDetails | null> {
  try {
    const response = await fetch(`${GHL_CONFIG.BASE_URL}/contacts/${contactId}`, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        Version: GHL_CONFIG.API_VERSION,
      },
    });

    if (!response.ok) {
      const t = await response.text();
      logger.warn("Failed to fetch GHL contact details", {
        contactId,
        status: response.status,
        body: t.slice(0, 500),
      });
      return null;
    }

    const contactData = await response.json();
    const contact = contactData?.contact || {};

    return {
      full_name: contact?.name || "Existing Contact",
      email: contact?.email || opts.fallbackEmail || null,
      phone: contact?.phone || opts.fallbackPhone || null,
      updated_at: contact?.dateUpdated || new Date().toISOString(),
    };
  } catch (err: any) {
    logger.warn("Failed to fetch GHL contact details (exception)", {
      contactId,
      error: err?.message ?? String(err),
    });
    return null;
  }
}

async function createGHLContact(data: ParsedCV, accessToken: string): Promise<string> {
  const { firstName, lastName } = splitName(data.full_name);

  const email = data.email && isValidEmail(data.email) ? data.email.trim() : undefined;
  const phone = data.phone ? normalizePhone(data.phone) ?? undefined : undefined;

  const basePayload: any = {
    firstName: firstName || "Unknown",
    lastName: lastName || "",
    locationId: ENV.GHL_LOCATION_ID,
    tags: GHL_CONFIG.DEFAULT_TAGS,
  };

  const variants: any[] = [
    { ...basePayload, ...(email ? { email } : {}), ...(phone ? { phone } : {}) },
    { ...basePayload, ...(email ? { email } : {}) },
    { ...basePayload, ...(phone ? { phone } : {}) },
    { ...basePayload },
  ];

  const url = `${GHL_CONFIG.BASE_URL}/contacts/`;
  const maxAttempts = GHL_CONFIG.MAX_RETRIES;

  for (const payload of variants) {
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        const response = await fetch(url, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${accessToken}`,
            "Content-Type": "application/json",
            Version: GHL_CONFIG.API_VERSION,
          },
          body: JSON.stringify(payload),
        });

        const bodyText = await response.text();

        if (response.ok) {
          let result: any;
          try {
            result = JSON.parse(bodyText);
          } catch {
            throw new Error(`GHL returned non-JSON: ${bodyText.slice(0, 400)}`);
          }

          const id = result?.contact?.id;
          if (!id) {
            throw new Error(`GHL succeeded but missing contact.id: ${bodyText.slice(0, 800)}`);
          }

          logger.info("GHL contact created successfully", {
            contactId: id,
            name: `${firstName} ${lastName}`,
          });
          return id;
        }

        const status = response.status;
        const retryable = isRetryableStatus(status, GHL_CONFIG.RETRYABLE_STATUS_CODES);

        logger.warn("GHL contact creation failed", {
          attempt,
          status,
          retryable,
          bodyPreview: bodyText.slice(0, 500),
        });

        if (!retryable) break;
        if (attempt === maxAttempts) break;

        const waitMs = getRetryDelay(attempt, GHL_CONFIG);
        await delay(waitMs);
      } catch (error: any) {
        if (attempt === maxAttempts) {
          throw error;
        }
        logger.warn("GHL contact creation exception", {
          attempt,
          error: error.message,
        });
        const waitMs = getRetryDelay(attempt, GHL_CONFIG);
        await delay(waitMs);
      }
    }
  }

  throw new Error("GHL contact creation failed after trying payload variants");
}

async function updateGHLContact(
  contactId: string,
  data: ParsedCV,
  accessToken: string
): Promise<void> {
  // 🔥 USE COMPLETE FIELD MAPPING
  const customFieldsData = buildCompleteGHLCustomFields(data);

  const customFields = Object.entries(customFieldsData).map(([key, value]) => ({
    key,
    value: value || "",
  }));

  const maxAttempts = GHL_CONFIG.MAX_RETRIES;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const response = await fetch(`${GHL_CONFIG.BASE_URL}/contacts/${contactId}`, {
        method: "PUT",
        headers: {
          Authorization: `Bearer ${accessToken}`,
          "Content-Type": "application/json",
          Version: GHL_CONFIG.API_VERSION,
        },
        body: JSON.stringify({ customFields }),
      });

      if (response.ok) {
        logger.info("GHL contact updated with all custom fields", {
          contactId,
          fieldsUpdated: customFields.length,
        });
        return;
      }

      const txt = await response.text();
      const status = response.status;
      const retryable = isRetryableStatus(status, GHL_CONFIG.RETRYABLE_STATUS_CODES);

      logger.warn("GHL contact update failed", {
        contactId,
        attempt,
        status,
        retryable,
        bodyPreview: txt.slice(0, 500),
      });

      if (!retryable || attempt === maxAttempts) {
        throw new Error(`GHL contact update failed: ${txt.slice(0, 800)}`);
      }

      const waitMs = getRetryDelay(attempt, GHL_CONFIG);
      await delay(waitMs);
    } catch (error: any) {
      if (attempt === maxAttempts) {
        throw error;
      }
      logger.warn("GHL contact update exception", {
        contactId,
        attempt,
        error: error.message,
      });
      const waitMs = getRetryDelay(attempt, GHL_CONFIG);
      await delay(waitMs);
    }
  }
}

function guessUploadMimeType(filename: string): string {
  const ext = filename.split(".").pop()?.toLowerCase();
  if (ext === "pdf") return "application/pdf";
  if (ext === "docx") return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
  if (ext === "doc") return "application/msword";
  if (ext === "txt") return "text/plain";
  return "application/octet-stream";
}

async function uploadCVToGHL(
  contactId: string,
  cvFilePath: string,
  originalFilename: string,
  accessToken: string
): Promise<{ success: boolean; fileId?: string; error?: string }> {
  try {
    const encodedPath = cvFilePath.split("/").map(encodeURIComponent).join("/");
    const downloadUrl = `${ENV.SUPABASE_URL}/storage/v1/object/${SUPABASE_CONFIG.STORAGE_BUCKET}/${encodedPath}`;

    const downloadResponse = await fetch(downloadUrl, {
      headers: {
        Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
        apikey: ENV.SUPABASE_SERVICE_KEY,
      },
    });

    if (!downloadResponse.ok) {
      const t = await downloadResponse.text();
      throw new Error(`Failed to download CV: ${downloadResponse.status} - ${t.slice(0, 300)}`);
    }

    const fileBuffer = await downloadResponse.arrayBuffer();

    const BlobCtor = (globalThis as any).Blob;
    const FormDataCtor = (globalThis as any).FormData;

    if (!BlobCtor || !FormDataCtor) {
      throw new Error("Blob/FormData not available in this runtime");
    }

    const mimeType = guessUploadMimeType(originalFilename);
    const blob = new BlobCtor([fileBuffer], { type: mimeType });
    const formData = new FormDataCtor();
    formData.append("file", blob, originalFilename);

    const uploadResponse = await fetch(`${GHL_CONFIG.BASE_URL}/contacts/${contactId}/files`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${accessToken}`,
        Version: GHL_CONFIG.API_VERSION,
      },
      body: formData as any,
    });

    if (!uploadResponse.ok) {
      const errorText = await uploadResponse.text();
      throw new Error(`GHL upload failed: ${uploadResponse.status} - ${errorText.slice(0, 800)}`);
    }

    const uploadResult = await uploadResponse.json();

    logger.info("CV uploaded to GHL successfully", {
      contactId,
      fileId: uploadResult?.id,
    });

    return {
      success: true,
      fileId: uploadResult?.id,
    };
  } catch (error: any) {
    logger.error("Failed to upload CV to GHL", {
      contactId,
      error: error?.message ?? String(error),
    });

    return {
      success: false,
      error: error?.message ?? String(error),
    };
  }
}

// ============================================
// DATABASE OPERATIONS
// ============================================

async function writeCandidate(candidateData: any): Promise<string> {
  const maxAttempts = SUPABASE_CONFIG.MAX_RETRIES;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const response = await fetch(`${ENV.SUPABASE_URL}/rest/v1/candidates`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
          apikey: ENV.SUPABASE_SERVICE_KEY,
          "Content-Type": "application/json",
          Prefer: "return=representation",
        },
        body: JSON.stringify(candidateData),
      });

      if (response.ok) {
        const result = await response.json();
        const id = result?.[0]?.id;
        if (!id) throw new Error("Candidate insert succeeded but no id returned");
        return id;
      }

      const errorText = await response.text();

      if (attempt === maxAttempts) {
        throw new Error(`Failed to write candidate: ${errorText.slice(0, 800)}`);
      }

      logger.warn("Retrying candidate insert", { attempt });
      await delay(getRetryDelay(attempt, SUPABASE_CONFIG));
    } catch (error: any) {
      if (attempt === maxAttempts) {
        throw error;
      }
      logger.warn("Candidate insert exception", {
        attempt,
        error: error.message,
      });
      await delay(getRetryDelay(attempt, SUPABASE_CONFIG));
    }
  }

  throw new Error("Failed to write candidate after retries");
}

async function updateCandidateGHL(
  candidateId: string,
  ghlContactId: string | null,
  status: string
): Promise<void> {
  const response = await fetch(`${ENV.SUPABASE_URL}/rest/v1/candidates?id=eq.${candidateId}`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
      apikey: ENV.SUPABASE_SERVICE_KEY,
      "Content-Type": "application/json",
      Prefer: "return=minimal",
    },
    body: JSON.stringify({
      ghl_contact_id: ghlContactId,
      status: status,
    }),
  });

  if (!response.ok) {
    const t = await response.text();
    throw new Error(`Failed to update candidate GHL/status: ${t.slice(0, 800)}`);
  }
}

async function updateBatchStatus(
  batchId: string,
  status: string,
  processedCount?: number
): Promise<void> {
  const updateData: any = { status };
  if (processedCount !== undefined) updateData.processed_count = processedCount;
  if (status === "complete" || status === "awaiting_input" || status === "failed")
    updateData.completed_at = new Date().toISOString();

  const response = await fetch(`${ENV.SUPABASE_URL}/rest/v1/processing_batches?id=eq.${batchId}`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
      apikey: ENV.SUPABASE_SERVICE_KEY,
      "Content-Type": "application/json",
      Prefer: "return=minimal",
    },
    body: JSON.stringify(updateData),
  });

  if (!response.ok) {
    const t = await response.text();
    throw new Error(`Failed to update batch status: ${response.status} ${t.slice(0, 800)}`);
  }
}

async function updateBatchProgress(
  batchId: string,
  processed: number,
  _total: number
): Promise<void> {
  const response = await fetch(`${ENV.SUPABASE_URL}/rest/v1/processing_batches?id=eq.${batchId}`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
      apikey: ENV.SUPABASE_SERVICE_KEY,
      "Content-Type": "application/json",
      Prefer: "return=minimal",
    },
    body: JSON.stringify({ processed_count: processed }),
  });

  if (!response.ok) {
    const t = await response.text();
    throw new Error(`Failed to update batch progress: ${response.status} ${t.slice(0, 800)}`);
  }
}

async function addSingleToHoldQueue(
  batchId: string,
  clientId: string,
  item: {
    file_name: string;
    file_path: string;
    extracted_name: string | null;
    raw_text: string;
    extracted_data?: any;
  }
): Promise<string> {
  const record = {
    batch_id: batchId,
    client_id: clientId,
    extracted_name: item.extracted_name,
    cv_file_path: item.file_path,
    cv_raw_text: item.raw_text,
    status: "pending",
    extraction_data: item.extracted_data || null,
    file_name: item.file_name,
  };

  const response = await fetch(`${ENV.SUPABASE_URL}/rest/v1/hold_queue`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
      apikey: ENV.SUPABASE_SERVICE_KEY,
      "Content-Type": "application/json",
      Prefer: "return=representation",
    },
    body: JSON.stringify(record),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to add to hold queue: ${errorText.slice(0, 800)}`);
  }

  const result = await response.json();
  const insertedId = result?.[0]?.id;

  logger.info("Added to hold_queue", {
    id: insertedId,
    file: item.file_name,
    batchId,
  });

  return insertedId;
}

async function recordRejection(
  batchId: string,
  fileName: string,
  filePath: string,
  classification: ClassificationResult
): Promise<void> {
  try {
    // Optional: Record rejections for analytics
    // You could create a rejections table or just log
    logger.info("Document rejected and logged", {
      batchId,
      fileName,
      type: classification.document_type,
      confidence: classification.confidence,
      reason: classification.rejection_reason,
    });
  } catch (error) {
    logger.warn("Failed to record rejection", {
      error,
    });
  }
}