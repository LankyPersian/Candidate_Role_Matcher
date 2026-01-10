// ============================================
// PROCESS CV BATCH - PRODUCTION VERSION
// ============================================
// Complete CV processing pipeline with:
// - Document classification (reject non-CVs)
// - Cost tracking and limits  
// - Per-file status tracking (idempotency)
// - Batch recovery support
// - Comprehensive error handling
// - Full 51-field GHL sync with file uploads
// - Duplicate detection (Supabase + GHL)
// - Hold queue management

import { task, logger } from "@trigger.dev/sdk";
import { Buffer } from "buffer";
import {
  ENV,
  PROCESSING_CONFIG,
  SUPABASE_CONFIG,
  GEMINI_CONFIG,
  GHL_CONFIG,
  VALIDATION_RULES,
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

interface BatchConfig {
  upload_type: 'general' | 'specific';
  job_id: string | null;
  required_skills: string[];
  exclude_students: boolean;
  colleague: string;
}

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

interface FileProcessingStatus {
  file_name: string;
  file_path: string;
  status: "pending" | "processing" | "complete" | "failed" | "rejected";
  error_message?: string;
  candidate_id?: string;
  processed_at?: string;
}

interface BatchStats {
  total_files: number;
  classified: number;
  rejected_by_classification: number;
  rejected_by_filters: number;
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
// BATCH CONFIGURATION
// ============================================

async function loadBatchConfig(batchId: string): Promise<BatchConfig> {
  const response = await fetch(
    `${ENV.SUPABASE_URL}/rest/v1/processing_batches?id=eq.${batchId}&select=upload_type,job_id,required_skills,exclude_students,colleague&limit=1`,
    {
      headers: {
        Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
        apikey: ENV.SUPABASE_SERVICE_KEY,
      },
    }
  );

  if (!response.ok) {
    throw new Error(`Failed to load batch config: ${response.status}`);
  }

  const data = await response.json();
  const config = data[0];

  return {
    upload_type: config?.upload_type || 'general',
    job_id: config?.job_id || null,
    required_skills: config?.required_skills || [],
    exclude_students: config?.exclude_students ?? false,
    colleague: config?.colleague || 'Unknown',
  };
}

async function recordFilterRejection(
  batchId: string,
  fileName: string,
  filePath: string,
  filterData: {
    reason: string;
    [key: string]: any;
  }
): Promise<void> {
  try {
    await fetch(`${ENV.SUPABASE_URL}/rest/v1/rejected_documents`, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
        apikey: ENV.SUPABASE_SERVICE_KEY,
        'Content-Type': 'application/json',
        Prefer: 'return=minimal',
      },
      body: JSON.stringify({
        batch_id: batchId,
        file_name: fileName,
        file_path: filePath,
        rejection_type: 'filter',
        rejection_reason: filterData.reason,
        classification_data: filterData,
      }),
    });
  } catch (error: any) {
    logger.warn('Failed to record filter rejection', {
      error: error.message,
    });
  }
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
      // 1) Check if batch is already processing or complete (idempotency)
      const batchStatus = await getBatchStatus(batchId);
      
      if (batchStatus === "complete") {
        logger.info("✅ Batch already complete - skipping", { batchId });
        return { processed: 0, failed: 0, held: 0, rejected: 0, batchId, skipped: true };
      }
      
      if (batchStatus === "processing") {
        // Check timeout
        const isTimedOut = await isBatchTimedOut(batchId);
        if (!isTimedOut) {
          logger.warn("⚠️ Batch still processing - skipping duplicate run", { batchId });
          return { processed: 0, failed: 0, held: 0, rejected: 0, batchId, skipped: true };
        }
        
        logger.warn("🔄 Batch timed out - attempting recovery", { batchId });
        await markBatchForRecovery(batchId);
      }

      // 2) List all files
      const files = await listBatchFiles(batchId, clientId);
      logger.info(`📁 Found ${files.length} files to process`, { batchId });

      if (files.length === 0) {
        logger.warn("No files found in batch", { batchId, clientId });
        await updateBatchStatus(batchId, "complete", 0);
        return { processed: 0, failed: 0, held: 0, rejected: 0, batchId };
      }

      // 3) Validate batch size
      if (files.length > PROCESSING_CONFIG.MAX_BATCH_SIZE) {
        logger.error("❌ Batch exceeds maximum size", {
          batchId,
          fileCount: files.length,
          maxSize: PROCESSING_CONFIG.MAX_BATCH_SIZE,
        });

        await updateBatchStatus(batchId, "failed", 0);
        throw new Error(`Batch size ${files.length} exceeds maximum ${PROCESSING_CONFIG.MAX_BATCH_SIZE}`);
      }

      // 4) Check if batch can be processed within cost limits
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

      // 5) Mark batch as processing
      await updateBatchStatus(batchId, "processing");

      // Load batch configuration
      const batchConfig = await loadBatchConfig(batchId);
      logger.info("✅ Batch configuration loaded", {
        batchId,
        uploadType: batchConfig.upload_type,
        jobId: batchConfig.job_id,
        requiredSkills: batchConfig.required_skills,
        excludeStudents: batchConfig.exclude_students,
        colleague: batchConfig.colleague,
      });

      // Initialize stats
      const stats: BatchStats = {
        total_files: files.length,
        classified: 0,
        rejected_by_classification: 0,
        rejected_by_filters: 0,
        processed: 0,
        failed: 0,
        held_for_review: 0,
        duplicates_found: 0,
      };

      const classificationResults: ClassificationResult[] = [];
      const ghlAccessToken = ENV.GHL_PRIVATE_KEY;

      // 6) Get file processing status (for recovery)
      const fileStatuses = await getFileProcessingStatuses(batchId);
      const fileStatusMap = new Map(fileStatuses.map(f => [f.file_path, f]));

      // 7) Process each file sequentially
      for (let i = 0; i < files.length; i++) {
        const file = files[i];
        const correlationId = `${batchId}_${file.name}`;

        try {
          // Check if file already processed (idempotency)
          const existingStatus = fileStatusMap.get(file.path);
          if (existingStatus?.status === "complete") {
            logger.info(`⏭️ File already processed - skipping`, {
              correlationId,
              fileName: file.name,
              candidateId: existingStatus.candidate_id,
            });
            stats.processed++;
            continue;
          }

          if (existingStatus?.status === "rejected") {
            logger.info(`⏭️ File already rejected - skipping`, {
              correlationId,
              fileName: file.name,
              reason: existingStatus.error_message,
            });
            stats.rejected_by_classification++;
            continue;
          }

          logger.info(`📄 Processing file ${i + 1}/${files.length}`, {
            correlationId,
            name: file.name,
            path: file.path,
          });

          // Mark file as processing
          await updateFileStatus(batchId, file.path, file.name, "processing");

          // Validate file size
          const fileSize = await getFileSize(file.path);
          if (fileSize > VALIDATION_RULES.MAX_FILE_SIZE_MB * 1024 * 1024) {
            logger.warn("❌ File too large - rejecting", {
              correlationId,
              fileName: file.name,
              sizeMB: (fileSize / 1024 / 1024).toFixed(2),
              maxMB: VALIDATION_RULES.MAX_FILE_SIZE_MB,
            });

            await updateFileStatus(
              batchId,
              file.path,
              file.name,
              "failed",
              `File size ${(fileSize / 1024 / 1024).toFixed(2)}MB exceeds maximum ${VALIDATION_RULES.MAX_FILE_SIZE_MB}MB`
            );
            stats.failed++;
            continue;
          }

          // Download file
          const fileBuffer = await downloadFile(file.path);

          // Extract text
          const rawText = await extractText(fileBuffer, file.name);
          await trackTextExtraction(batchId, rawText?.length || 0);

          logger.info("Extracted text stats", {
            correlationId,
            file: file.name,
            length: rawText?.length ?? 0,
            preview: (rawText || "").slice(0, 200),
          });

          // Validate text length
          if (!rawText || rawText.trim().length < PROCESSING_CONFIG.MIN_TEXT_LENGTH_REQUIRED) {
            logger.warn("❌ File has insufficient text - rejecting", {
              correlationId,
              fileName: file.name,
              textLength: rawText?.trim().length || 0,
              required: PROCESSING_CONFIG.MIN_TEXT_LENGTH_REQUIRED,
            });

            await updateFileStatus(
              batchId,
              file.path,
              file.name,
              "failed",
              "Insufficient text content"
            );
            await recordRejection(
              batchId,
              file.name,
              file.path,
              {
                document_type: "other",
                confidence: 1.0,
                reasoning: "Insufficient text content",
                should_process: false,
                key_indicators: ["insufficient_text"],
                rejection_reason: "Text too short",
              }
            );
            stats.failed++;
            continue;
          }

          // 🔥 DOCUMENT CLASSIFICATION (cost optimization + security)
          logger.info("🔍 Classifying document", {
            correlationId,
            fileName: file.name,
          });

          // Quick heuristic check first (free)
          const heuristic = quickHeuristicCheck(rawText, file.name);
          logger.info("Heuristic check result", {
            correlationId,
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
              correlationId,
              file: file.name,
              type: classification.document_type,
              confidence: `${(classification.confidence * 100).toFixed(1)}%`,
              reason: classification.rejection_reason,
            });

            stats.rejected_by_classification++;

            // Log rejection to database for tracking
            await recordRejection(batchId, file.name, file.path, classification);
            await updateFileStatus(
              batchId,
              file.path,
              file.name,
              "rejected",
              classification.rejection_reason
            );

            continue;
          }

          logger.info("✅ Document classified as CV - processing", {
            correlationId,
            file: file.name,
            confidence: `${(classification.confidence * 100).toFixed(1)}%`,
          });

          // Quick parse for contact info
          const quickData = await quickParse(rawText);
          await trackQuickParse(batchId);

          logger.info("Quick parse extracted", {
            correlationId,
            email: quickData.email || "none",
            phone: quickData.phone || "none",
            name: quickData.full_name || "none",
          });

          // PRE-QUALIFICATION FILTERING
          // Check student status
          if (batchConfig.exclude_students && quickData.is_student === true) {
            logger.warn("❌ Rejected by filter - student status", {
              correlationId,
              fileName: file.name,
            });

            stats.rejected_by_filters++;
            await recordFilterRejection(batchId, file.name, file.path, {
              reason: "Currently enrolled as student (excluded by filter)",
              is_student: true,
            });

            await updateFileStatus(
              batchId,
              file.path,
              file.name,
              "rejected",
              "Currently enrolled as student"
            );

            continue;
          }

          // Check required skills
          if (batchConfig.required_skills.length > 0) {
            const candidateSkills = quickData.skills || [];
            const hasRequiredSkills = batchConfig.required_skills.some(requiredSkill =>
              candidateSkills.some(skill =>
                skill.toLowerCase().includes(requiredSkill.toLowerCase())
              )
            );

            if (!hasRequiredSkills) {
              const missingSkills = batchConfig.required_skills.filter(req =>
                !candidateSkills.some(cs => cs.toLowerCase().includes(req.toLowerCase()))
              );

              logger.warn("❌ Rejected by filter - missing required skills", {
                correlationId,
                fileName: file.name,
                required: batchConfig.required_skills,
                found: candidateSkills,
                missing: missingSkills,
              });

              stats.rejected_by_filters++;
              await recordFilterRejection(batchId, file.name, file.path, {
                reason: `Missing required skills: ${missingSkills.join(', ')}`,
                required_skills: batchConfig.required_skills,
                candidate_skills: candidateSkills,
                missing_skills: missingSkills,
              });

              await updateFileStatus(
                batchId,
                file.path,
                file.name,
                "rejected",
                `Missing required skills: ${missingSkills.join(', ')}`
              );

              continue;
            }
          }

          // Check for required contact info
          const hasEmail = quickData.email && isValidEmail(quickData.email);
          const hasPhone = quickData.phone && normalizePhone(quickData.phone);

          if (PROCESSING_CONFIG.REQUIRE_EMAIL_OR_PHONE && !hasEmail && !hasPhone) {
            logger.warn("⏸️ Missing contact info - sending to hold queue", {
              correlationId,
              fileName: file.name,
            });

            stats.held_for_review++;
            await addSingleToHoldQueue(batchId, clientId, {
              file_name: file.name,
              file_path: file.path,
              extracted_name: quickData.full_name,
              raw_text: rawText,
              extracted_data: {
                ...quickData,
                reason: "missing_contact_info",
              },
            });

            await updateFileStatus(
              batchId,
              file.path,
              file.name,
              "complete",
              undefined,
              undefined,
              "Sent to hold queue - missing contact info"
            );

            continue;
          }

          // Duplicate detection
          const existingCandidate = await findExistingCandidate(quickData.email, quickData.phone);
          const ghlDuplicate = await findExistingGHLContact(
            quickData.email,
            quickData.phone,
            ghlAccessToken
          );
          await trackGHLCall("search_contact", batchId);

          if (existingCandidate || ghlDuplicate) {
            logger.warn("🔍 Duplicate detected", {
              correlationId,
              fileName: file.name,
              supabaseDuplicate: !!existingCandidate,
              ghlDuplicate: !!ghlDuplicate,
            });

            stats.duplicates_found++;
            stats.held_for_review++;

            await addSingleToHoldQueue(batchId, clientId, {
              file_name: file.name,
              file_path: file.path,
              extracted_name: quickData.full_name,
              raw_text: rawText,
              extracted_data: {
                ...quickData,
                reason: "duplicate_detected",
                supabase_duplicate_id: existingCandidate?.id,
                ghl_duplicate_contact_id: ghlDuplicate,
              },
            });

            await updateFileStatus(
              batchId,
              file.path,
              file.name,
              "complete",
              undefined,
              undefined,
              "Sent to hold queue - duplicate detected"
            );

            continue;
          }

          // Full parse
          const parsedData = await fullParse(rawText);
          parsedData.cv_raw_text = rawText;
          await trackFullParse(batchId);

          // Merge quick parse contact info (in case full parse missed it)
          parsedData.email = parsedData.email || quickData.email;
          parsedData.phone = parsedData.phone || quickData.phone;
          parsedData.full_name = parsedData.full_name || quickData.full_name;

          logger.info("Full parse complete", {
            correlationId,
            fileName: file.name,
            fieldsExtracted: Object.keys(parsedData).filter(k => parsedData[k as keyof ParsedCV]).length,
          });

          // Write to Supabase (fail-safe: always save here first)
          const candidateId = await writeCandidate({
            ...parsedData,
            client_id: clientId || null,
            ghl_contact_id: null,
            cv_file_path: file.path,
            batch_id: batchId,
            status: "pending_ghl_sync",
          });

          logger.info("✅ Candidate saved to Supabase", {
            correlationId,
            candidateId,
            fileName: file.name,
          });

          // Sync to GHL
          try {
            // Create or update GHL contact
            const existingGHLContactId = await findExistingGHLContact(
              parsedData.email,
              parsedData.phone,
              ghlAccessToken
            );
            await trackGHLCall("search_contact", batchId);

            let ghlContactId: string;

            if (existingGHLContactId) {
              ghlContactId = existingGHLContactId;
              logger.info("📝 Updating existing GHL contact", {
                correlationId,
                ghlContactId,
              });
            } else {
              ghlContactId = await createGHLContact(parsedData, ghlAccessToken);
              await trackGHLCall("create_contact", batchId);
              logger.info("✅ Created new GHL contact", {
                correlationId,
                ghlContactId,
              });
            }

            // Upload CV file to GHL
            let cvFileUrl: string | null = null;
            try {
              const cvUploadResult = await uploadCVToGHL(
                ghlContactId,
                file.path,
                file.name,
                ghlAccessToken
              );
              await trackGHLCall("upload_file", batchId);

              cvFileUrl = cvUploadResult.fileUrl || null;

              if (!cvUploadResult.success) {
                logger.warn("⚠️ CV file upload failed", {
                  correlationId,
                  ghlContactId,
                  error: cvUploadResult.error,
                });
              } else {
                logger.info("✅ CV file uploaded to GHL", {
                  correlationId,
                  ghlContactId,
                  fileUrl: cvFileUrl,
                });
              }
            } catch (uploadError: any) {
              logger.error("❌ CV file upload exception", {
                correlationId,
                ghlContactId,
                error: uploadError.message,
              });
            }

            // Upload cover letter (if exists)
            let coverLetterUrl: string | null = null;
            try {
              coverLetterUrl = await uploadCoverLetterToGHL(
                ghlContactId,
                file.path,
                ghlAccessToken
              );
              if (coverLetterUrl) {
                await trackGHLCall("upload_file", batchId);
                logger.info("✅ Cover letter uploaded", {
                  correlationId,
                  ghlContactId,
                  fileUrl: coverLetterUrl,
                });
              }
            } catch (clError: any) {
              logger.warn("⚠️ Cover letter upload failed", {
                correlationId,
                error: clError.message,
              });
            }

            // Upload other documents (if exists)
            let otherDocsUrl: string | null = null;
            try {
              otherDocsUrl = await uploadOtherDocsToGHL(
                ghlContactId,
                file.path,
                ghlAccessToken
              );
              if (otherDocsUrl) {
                await trackGHLCall("upload_file", batchId);
                logger.info("✅ Other documents uploaded", {
                  correlationId,
                  ghlContactId,
                  fileUrl: otherDocsUrl,
                });
              }
            } catch (odError: any) {
              logger.warn("⚠️ Other documents upload failed", {
                correlationId,
                error: odError.message,
              });
            }

            // Update GHL contact with all fields + file URLs
            await updateGHLContact(
              ghlContactId,
              parsedData,
              candidateId,
              cvFileUrl,
              coverLetterUrl,
              otherDocsUrl,
              ghlAccessToken
            );
            await trackGHLCall("update_contact", batchId);

            logger.info("✅ GHL contact updated with full data", {
              correlationId,
              ghlContactId,
              cvUploaded: !!cvFileUrl,
              coverLetterUploaded: !!coverLetterUrl,
              otherDocsUploaded: !!otherDocsUrl,
            });

            // Update candidate with GHL contact ID
            await updateCandidateGHL(candidateId, ghlContactId, "complete");

            stats.processed++;

            // Mark file as complete
            await updateFileStatus(
              batchId,
              file.path,
              file.name,
              "complete",
              undefined,
              candidateId
            );

            logger.info("✅ File processing complete", {
              correlationId,
              candidateId,
              ghlContactId,
            });
          } catch (ghlError: any) {
            logger.error("❌ GHL sync failed", {
              correlationId,
              error: ghlError.message,
              stack: ghlError.stack,
              candidateId,
            });

            // Mark candidate as GHL sync failed (but data is safe in Supabase)
            await updateCandidateGHL(candidateId, null, "ghl_sync_failed");

            // Mark file as complete with warning
            await updateFileStatus(
              batchId,
              file.path,
              file.name,
              "complete",
              `GHL sync failed: ${ghlError.message}`,
              candidateId
            );

            stats.processed++; // Still count as processed (saved to Supabase)
          }

          // Small delay between files
          if (i < files.length - 1) {
            await delay(PROCESSING_CONFIG.DELAY_BETWEEN_FILES_MS);
          }

          // Update batch progress
          await updateBatchProgress(batchId, stats.processed + stats.held_for_review + stats.failed + stats.rejected_by_classification + stats.rejected_by_filters, files.length);
        } catch (fileError: any) {
          logger.error("❌ File processing exception", {
            correlationId,
            fileName: file.name,
            error: fileError.message,
            stack: fileError.stack,
          });

          stats.failed++;

          // Mark file as failed
          await updateFileStatus(
            batchId,
            file.path,
            file.name,
            "failed",
            fileError.message
          );

          // Continue to next file (don't fail entire batch)
          if (PROCESSING_CONFIG.CONTINUE_ON_FILE_ERROR) {
            continue;
          } else {
            throw fileError;
          }
        }
      }

      // Log classification stats
      if (classificationResults.length > 0) {
        logClassificationStats(classificationResults, batchId);
      }

      // Final batch status
      const finalStatus = stats.held_for_review > 0 ? "awaiting_input" : "complete";
      await updateBatchStatus(batchId, finalStatus, stats.processed + stats.failed + stats.held_for_review + stats.rejected_by_classification + stats.rejected_by_filters);

      // Log cost summary
      await logBatchCostSummary(batchId);

      logger.info("🎉 Batch processing complete", {
        batchId,
        stats,
        finalStatus,
      });

      return {
        ...stats,
        batchId,
        status: finalStatus,
      };
    } catch (error: any) {
      logger.error("❌ Batch processing failed", {
        batchId,
        error: error.message,
        stack: error.stack,
      });

      // Mark batch as failed
      await updateBatchStatus(batchId, "failed");

      throw error;
    }
  },
});

// ============================================
// GEMINI API - TEXT EXTRACTION
// ============================================

async function extractText(fileBuffer: ArrayBuffer, fileName: string): Promise<string> {
  const base64Data = Buffer.from(fileBuffer).toString("base64");

  const ext = fileName.split(".").pop()?.toLowerCase();
  let mimeType = "application/pdf";

  if (ext === "docx") {
    mimeType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
  } else if (ext === "doc") {
    mimeType = "application/msword";
  }

  const url = `${GEMINI_CONFIG.BASE_URL}/models/${GEMINI_CONFIG.MODEL}:generateContent?key=${ENV.GEMINI_API_KEY}`;
  const maxAttempts = GEMINI_CONFIG.MAX_RETRIES;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const response = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          contents: [
            {
              parts: [
                {
                  inlineData: {
                    mimeType: mimeType,
                    data: base64Data,
                  },
                },
                {
                  text: "Extract all text from this document. Return only the raw text content, preserving structure and formatting.",
                },
              ],
            },
          ],
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

      logger.warn("Gemini text extraction failed", {
        attempt,
        status,
        retryable,
        bodyPreview: bodyText.slice(0, 500),
      });

      if (!retryable || attempt === maxAttempts) {
        throw new Error(`Gemini request failed (status ${status}): ${bodyText.slice(0, 500)}`);
      }

      const waitMs = getRetryDelay(attempt, GEMINI_CONFIG);
      logger.info("Retrying Gemini text extraction", { attempt, waitMs });
      await delay(waitMs);
    } catch (error: any) {
      if (attempt === maxAttempts) {
        throw error;
      }
      logger.warn("Gemini text extraction exception", {
        attempt,
        error: error.message,
      });
      const waitMs = getRetryDelay(attempt, GEMINI_CONFIG);
      await delay(waitMs);
    }
  }

  throw new Error("Gemini text extraction failed after retries");
}

// ============================================
// GEMINI API - QUICK PARSE (contact info only)
// ============================================

function stripJsonFences(s: string): string {
  let t = (s || "").trim();
  if (t.startsWith("```")) {
    t = t.replace(/^```[a-zA-Z]*\n?/, "").replace(/```$/, "").trim();
  }
  return t;
}

async function quickParse(rawText: string): Promise<Partial<ParsedCV> & { is_student?: boolean }> {
  const sampleText = rawText.substring(0, PROCESSING_CONFIG.MAX_TEXT_LENGTH_FOR_PARSE);

  const prompt = `Extract ONLY the following fields from this CV/resume. Return ONLY valid JSON in this exact format:

{
  "full_name": "string or null",
  "email": "string or null",
  "phone": "string or null",
  "is_student": "boolean - true if currently enrolled in education/university/college",
  "skills": ["array of skill strings - technical skills, programming languages, tools, frameworks"]
}

CV Text:
${sampleText}

Return ONLY the JSON object, no other text.`;

  const url = `${GEMINI_CONFIG.BASE_URL}/models/${GEMINI_CONFIG.MODEL}:generateContent?key=${ENV.GEMINI_API_KEY}`;

  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      contents: [{ parts: [{ text: prompt }] }],
      generationConfig: {
        temperature: 0,
        responseMimeType: "application/json",
      },
    }),
  });

  if (!response.ok) {
    const txt = await response.text();
    throw new Error(`Gemini quick parse failed: ${txt.slice(0, 500)}`);
  }

  const data = await response.json();
  const rawOutput = data?.candidates?.[0]?.content?.parts?.[0]?.text ?? "{}";
  const cleaned = stripJsonFences(rawOutput);

  try {
    const parsed = JSON.parse(cleaned);
    return {
      full_name: parsed.full_name || null,
      email: parsed.email || null,
      phone: parsed.phone || null,
      is_student: parsed.is_student ?? false,
      skills: Array.isArray(parsed.skills) ? parsed.skills : [],
    };
  } catch {
    logger.warn("Failed to parse Gemini quick parse output", {
      outputPreview: truncateForLog(cleaned, 500),
    });
    return { 
      full_name: null, 
      email: null, 
      phone: null,
      is_student: false,
      skills: []
    };
  }
}

// ============================================
// GEMINI API - FULL PARSE (all 51 fields)
// ============================================

async function fullParse(rawText: string): Promise<ParsedCV> {
  const sampleText = rawText.substring(0, PROCESSING_CONFIG.MAX_TEXT_LENGTH_FOR_PARSE);

  const prompt = `Extract ALL structured data from this CV/resume. Return ONLY valid JSON in this exact format:

{
  "full_name": "string or null",
  "email": "string or null",
  "phone": "string or null",
  "address": "string or null",
  "linkedin_url": "string or null",
  "date_of_birth": "YYYY-MM-DD or null",
  "nationality": "string or null",
  "visa_work_permit": "string or null",
  "professional_summary": "string or null",
  "future_job_aspirations": "string or null",
  "work_history": [
    {
      "job_title": "string",
      "company_name": "string",
      "company_location": "string or null",
      "start_date": "string",
      "end_date": "string or Present",
      "employment_type": "string or null",
      "duties_responsibilities": "string or null",
      "achievements": "string or null",
      "reason_for_leaving": "string or null"
    }
  ],
  "education": [
    {
      "qualification_name": "string",
      "institution_name": "string",
      "start_date": "string or null",
      "end_date": "string or null",
      "grade_classification": "string or null",
      "honours_awards": "string or null",
      "dissertation_thesis": "string or null",
      "extracurricular": "string or null"
    }
  ],
  "skills": ["string"],
  "certifications": [
    {
      "name": "string",
      "issuing_organisation": "string or null",
      "date_obtained": "string or null",
      "expiry_date": "string or null",
      "certification_id": "string or null"
    }
  ],
  "driving_licence": "string or null",
  "languages": [
    {
      "language": "string",
      "proficiency": "string or null",
      "reading": "string or null",
      "writing": "string or null",
      "speaking": "string or null",
      "certifications": "string or null"
    }
  ],
  "training_courses": ["string"],
  "professional_memberships": ["string"],
  "awards_honours": ["string"],
  "volunteering": ["string"],
  "interests_hobbies": ["string"],
  "candidate_references": ["string"],
  "military_service": "string or null",
  "salary_expectation": "string or null",
  "notice_period": "string or null",
  "availability_start_date": "YYYY-MM-DD or null",
  "relocation_willingness": "string or null",
  "remote_work_preference": "string or null",
  "cv_summary": "string or null"
}

CV Text:
${sampleText}

Return ONLY the JSON object, no other text.`;

  const url = `${GEMINI_CONFIG.BASE_URL}/models/${GEMINI_CONFIG.MODEL}:generateContent?key=${ENV.GEMINI_API_KEY}`;

  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      contents: [{ parts: [{ text: prompt }] }],
      generationConfig: {
        temperature: 0,
        responseMimeType: "application/json",
      },
    }),
  });

  if (!response.ok) {
    const txt = await response.text();
    throw new Error(`Gemini full parse failed: ${txt.slice(0, 500)}`);
  }

  const data = await response.json();
  const rawOutput = data?.candidates?.[0]?.content?.parts?.[0]?.text ?? "{}";
  const cleaned = stripJsonFences(rawOutput);

  try {
    const parsed = JSON.parse(cleaned);
    return {
      ...getEmptyParsedCV(),
      ...parsed,
    };
  } catch {
    logger.error("Failed to parse Gemini full parse output", {
      outputPreview: truncateForLog(cleaned, 1000),
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
// DUPLICATE DETECTION
// ============================================

async function findExistingCandidate(
  email: string | null,
  phone: string | null
): Promise<{ id: string; updated_at: string } | null> {
  if (email && isValidEmail(email)) {
    const response = await fetch(
      `${ENV.SUPABASE_URL}/rest/v1/candidates?email=eq.${encodeURIComponent(email)}&select=id,updated_at&order=updated_at.desc&limit=1`,
      {
        headers: {
          Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
          apikey: ENV.SUPABASE_SERVICE_KEY,
        },
      }
    );

    if (response.ok) {
      const data = await response.json();
      if (data.length > 0) {
        return data[0];
      }
    }
  }

  if (phone) {
    const normalized = normalizePhone(phone);
    if (normalized) {
      const response = await fetch(
        `${ENV.SUPABASE_URL}/rest/v1/candidates?phone=eq.${encodeURIComponent(normalized)}&select=id,updated_at&order=updated_at.desc&limit=1`,
        {
          headers: {
            Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
            apikey: ENV.SUPABASE_SERVICE_KEY,
          },
        }
      );

      if (response.ok) {
        const data = await response.json();
        if (data.length > 0) {
          return data[0];
        }
      }
    }
  }

  return null;
}

async function findExistingGHLContact(
  email: string | null,
  phone: string | null,
  accessToken: string
): Promise<string | null> {
  if (email && isValidEmail(email)) {
    const emailUrl = `${GHL_CONFIG.BASE_URL}/contacts/?locationId=${ENV.GHL_LOCATION_ID}&query=${encodeURIComponent(email)}`;

    const emailResponse = await fetch(emailUrl, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        Version: GHL_CONFIG.API_VERSION,
      },
    });

    if (emailResponse.ok) {
      const data = await emailResponse.json();
      if (data.contacts && data.contacts.length > 0) {
        return data.contacts[0].id;
      }
    }
  }

  if (phone) {
    const normalized = normalizePhone(phone);
    if (normalized) {
      const phoneUrl = `${GHL_CONFIG.BASE_URL}/contacts/?locationId=${ENV.GHL_LOCATION_ID}&query=${encodeURIComponent(normalized)}`;

      const phoneResponse = await fetch(phoneUrl, {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          Version: GHL_CONFIG.API_VERSION,
        },
      });

      if (phoneResponse.ok) {
        const data = await phoneResponse.json();
        if (data.contacts && data.contacts.length > 0) {
          return data.contacts[0].id;
        }
      }
    }
  }

  return null;
}

// ============================================
// GHL OPERATIONS
// ============================================

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

          logger.info("GHL contact created successfully", { contactId: id });
          return id;
        }

        const status = response.status;
        const retryable = isRetryableStatus(status, GHL_CONFIG.RETRYABLE_STATUS_CODES);

        if (!retryable) break;
        if (attempt === maxAttempts) break;

        const waitMs = getRetryDelay(attempt, GHL_CONFIG);
        await delay(waitMs);
      } catch (error: any) {
        if (attempt === maxAttempts) {
          throw error;
        }
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
  candidateId: string | undefined,
  cvFileUrl: string | null | undefined,
  coverLetterUrl: string | null | undefined,
  otherDocsUrl: string | null | undefined,
  accessToken: string
): Promise<void> {
  const customFieldsData = buildCompleteGHLCustomFields(
    data,
    candidateId,
    cvFileUrl,
    coverLetterUrl,
    otherDocsUrl
  );

  const customFields = Object.entries(customFieldsData).map(([key, value]) => ({
    key,
    value: value || "",
  }));

  const response = await fetch(`${GHL_CONFIG.BASE_URL}/contacts/${contactId}`, {
    method: "PUT",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json",
      Version: GHL_CONFIG.API_VERSION,
    },
    body: JSON.stringify({ customFields }),
  });

  if (!response.ok) {
    const txt = await response.text();
    throw new Error(`GHL contact update failed: ${txt.slice(0, 800)}`);
  }
}

// ============================================
// GHL FILE UPLOADS
// ============================================

function guessUploadMimeType(filename: string): string {
  const ext = filename.split(".").pop()?.toLowerCase();
  if (ext === "pdf") return "application/pdf";
  if (ext === "docx")
    return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
  if (ext === "doc") return "application/msword";
  if (ext === "txt") return "text/plain";
  return "application/octet-stream";
}

function encodeStoragePath(path: string): string {
  return path.split("/").map(encodeURIComponent).join("/");
}

/**
 * Upload CV file to GHL and return file URL
 */
async function uploadCVToGHL(
  contactId: string,
  cvFilePath: string,
  originalFilename: string,
  accessToken: string
): Promise<{ success: boolean; fileUrl?: string; error?: string }> {
  try {
    const encodedPath = encodeStoragePath(cvFilePath);
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
    formData.append("name", originalFilename);

    // FIXED: Correct endpoint for media upload
    const uploadResponse = await fetch(`${GHL_CONFIG.BASE_URL}/medias/upload-file`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${accessToken}`,
        Version: GHL_CONFIG.API_VERSION,
      },
      body: formData as any,
    });

    if (!uploadResponse.ok) {
      const errorText = await uploadResponse.text();
      throw new Error(
        `GHL CV upload failed: ${uploadResponse.status} - ${errorText.slice(0, 800)}`
      );
    }

    const uploadResult = await uploadResponse.json();
    const fileUrl = uploadResult?.url || uploadResult?.fileUrl || uploadResult?.publicUrl || null;

    return {
      success: true,
      fileUrl,
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

/**
 * Upload cover letter to GHL
 * Tries multiple folder patterns: cover_letters/, cover_letter/, coverletter/
 */
async function uploadCoverLetterToGHL(
  contactId: string,
  cvFilePath: string,
  accessToken: string
): Promise<string | null> {
  try {
    const pathParts = cvFilePath.split("/");
    const fileName = pathParts[pathParts.length - 1];
    const baseName = fileName.replace(/\.(pdf|docx?|txt)$/i, "");
    const clientId = pathParts[0];
    const batchId = pathParts[1];

    const possiblePaths = [
      `${clientId}/${batchId}/cover_letters/${baseName}_cover_letter.pdf`,
      `${clientId}/${batchId}/cover_letters/${baseName}.pdf`,
      `${clientId}/${batchId}/cover_letters/cover_letter.pdf`,
      `${clientId}/${batchId}/cover_letter/${baseName}_cover_letter.pdf`,
      `${clientId}/${batchId}/coverletter/${baseName}_cover_letter.pdf`,
    ];

    for (const clPath of possiblePaths) {
      const encodedPath = encodeStoragePath(clPath);
      const downloadUrl = `${ENV.SUPABASE_URL}/storage/v1/object/${SUPABASE_CONFIG.STORAGE_BUCKET}/${encodedPath}`;

      const downloadResponse = await fetch(downloadUrl, {
        headers: {
          Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
          apikey: ENV.SUPABASE_SERVICE_KEY,
        },
      });

      if (downloadResponse.ok) {
        const clData = await downloadResponse.arrayBuffer();

        const BlobCtor = (globalThis as any).Blob;
        const FormDataCtor = (globalThis as any).FormData;

        const blob = new BlobCtor([clData], { type: "application/pdf" });
        const formData = new FormDataCtor();
        formData.append("file", blob, "cover_letter.pdf");
        formData.append("name", "cover_letter.pdf");

        const uploadResponse = await fetch(`${GHL_CONFIG.BASE_URL}/medias/upload-file`, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${accessToken}`,
            Version: GHL_CONFIG.API_VERSION,
          },
          body: formData as any,
        });

        if (uploadResponse.ok) {
          const uploadResult = await uploadResponse.json();
          const fileUrl = uploadResult?.url || uploadResult?.fileUrl || uploadResult?.publicUrl || null;

          logger.info("✅ Cover letter uploaded", {
            contactId,
            fileUrl,
          });

          return fileUrl;
        }
      }
    }

    return null;
  } catch (error: any) {
    logger.warn("⚠️ Failed to upload cover letter", {
      contactId,
      error: error?.message ?? String(error),
    });
    return null;
  }
}

/**
 * Upload other documents to GHL
 * Tries multiple folder patterns: other_docs/, other_documents/, application/
 */
async function uploadOtherDocsToGHL(
  contactId: string,
  cvFilePath: string,
  accessToken: string
): Promise<string | null> {
  try {
    const pathParts = cvFilePath.split("/");
    const fileName = pathParts[pathParts.length - 1];
    const baseName = fileName.replace(/\.(pdf|docx?|txt)$/i, "");
    const clientId = pathParts[0];
    const batchId = pathParts[1];

    const possiblePaths = [
      `${clientId}/${batchId}/other_docs/${baseName}_application.pdf`,
      `${clientId}/${batchId}/other_docs/${baseName}.pdf`,
      `${clientId}/${batchId}/other_docs/application.pdf`,
      `${clientId}/${batchId}/other_documents/${baseName}_application.pdf`,
      `${clientId}/${batchId}/application/${baseName}_application.pdf`,
    ];

    for (const docPath of possiblePaths) {
      const encodedPath = encodeStoragePath(docPath);
      const downloadUrl = `${ENV.SUPABASE_URL}/storage/v1/object/${SUPABASE_CONFIG.STORAGE_BUCKET}/${encodedPath}`;

      const downloadResponse = await fetch(downloadUrl, {
        headers: {
          Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
          apikey: ENV.SUPABASE_SERVICE_KEY,
        },
      });

      if (downloadResponse.ok) {
        const docData = await downloadResponse.arrayBuffer();

        const BlobCtor = (globalThis as any).Blob;
        const FormDataCtor = (globalThis as any).FormData;

        const blob = new BlobCtor([docData], { type: "application/pdf" });
        const formData = new FormDataCtor();
        formData.append("file", blob, "application.pdf");
        formData.append("name", "application.pdf");

        const uploadResponse = await fetch(`${GHL_CONFIG.BASE_URL}/medias/upload-file`, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${accessToken}`,
            Version: GHL_CONFIG.API_VERSION,
          },
          body: formData as any,
        });

        if (uploadResponse.ok) {
          const uploadResult = await uploadResponse.json();
          const fileUrl = uploadResult?.url || uploadResult?.fileUrl || uploadResult?.publicUrl || null;

          logger.info("✅ Other documents uploaded", {
            contactId,
            fileUrl,
          });

          return fileUrl;
        }
      }
    }

    return null;
  } catch (error: any) {
    logger.warn("⚠️ Failed to upload other documents", {
      contactId,
      error: error?.message ?? String(error),
    });
    return null;
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
    const response = await fetch(`${ENV.SUPABASE_URL}/rest/v1/rejected_documents`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
        apikey: ENV.SUPABASE_SERVICE_KEY,
        "Content-Type": "application/json",
        Prefer: "return=minimal",
      },
      body: JSON.stringify({
        batch_id: batchId,
        file_name: fileName,
        file_path: filePath,
        document_type: classification.document_type,
        confidence: classification.confidence,
        rejection_reason: classification.rejection_reason,
        classification_data: {
          reasoning: classification.reasoning,
          key_indicators: classification.key_indicators,
        },
      }),
    });

    if (!response.ok) {
      logger.warn("Failed to record rejection", {
        status: response.status,
      });
    }
  } catch (error: any) {
    logger.warn("Exception recording rejection", {
      error: error.message,
    });
  }
}

// ============================================
// FILE STATUS TRACKING (IDEMPOTENCY)
// ============================================

async function updateFileStatus(
  batchId: string,
  filePath: string,
  fileName: string,
  status: "pending" | "processing" | "complete" | "failed" | "rejected",
  errorMessage?: string,
  candidateId?: string,
  notes?: string
): Promise<void> {
  const record: any = {
    batch_id: batchId,
    file_path: filePath,
    file_name: fileName,
    status,
    last_updated: new Date().toISOString(),
  };

  if (errorMessage) record.error_message = errorMessage;
  if (candidateId) record.candidate_id = candidateId;
  if (notes) record.notes = notes;
  if (status === "complete" || status === "failed" || status === "rejected") {
    record.processed_at = new Date().toISOString();
  }

  // Upsert (insert or update)
  const response = await fetch(`${ENV.SUPABASE_URL}/rest/v1/file_processing_status`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
      apikey: ENV.SUPABASE_SERVICE_KEY,
      "Content-Type": "application/json",
      Prefer: "resolution=merge-duplicates",
    },
    body: JSON.stringify(record),
  });

  if (!response.ok) {
    const txt = await response.text();
    logger.warn("Failed to update file status", {
      status: response.status,
      error: txt.slice(0, 500),
      filePath,
    });
  }
}

async function getFileProcessingStatuses(
  batchId: string
): Promise<FileProcessingStatus[]> {
  const response = await fetch(
    `${ENV.SUPABASE_URL}/rest/v1/file_processing_status?batch_id=eq.${batchId}&select=*`,
    {
      headers: {
        Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
        apikey: ENV.SUPABASE_SERVICE_KEY,
      },
    }
  );

  if (!response.ok) {
    return [];
  }

  return await response.json();
}

// ============================================
// BATCH RECOVERY
// ============================================

async function getBatchStatus(batchId: string): Promise<string | null> {
  const response = await fetch(
    `${ENV.SUPABASE_URL}/rest/v1/processing_batches?id=eq.${batchId}&select=status&limit=1`,
    {
      headers: {
        Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
        apikey: ENV.SUPABASE_SERVICE_KEY,
      },
    }
  );

  if (!response.ok) {
    return null;
  }

  const data = await response.json();
  return data[0]?.status || null;
}

async function isBatchTimedOut(batchId: string): Promise<boolean> {
  const response = await fetch(
    `${ENV.SUPABASE_URL}/rest/v1/processing_batches?id=eq.${batchId}&select=created_at,file_count&limit=1`,
    {
      headers: {
        Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
        apikey: ENV.SUPABASE_SERVICE_KEY,
      },
    }
  );

  if (!response.ok) {
    return false;
  }

  const data = await response.json();
  const batch = data[0];

  if (!batch) return false;

  const createdAt = new Date(batch.created_at).getTime();
  const now = Date.now();
  const elapsedMs = now - createdAt;

  // Calculate timeout based on file count
  const fileCount = batch.file_count || 100;
  const timeoutMs = Math.min(
    fileCount * 10000 + 300000, // 10s per file + 5 min buffer
    PROCESSING_CONFIG.MAX_BATCH_DURATION_MINUTES * 60000
  );

  return elapsedMs > timeoutMs;
}

async function markBatchForRecovery(batchId: string): Promise<void> {
  const response = await fetch(`${ENV.SUPABASE_URL}/rest/v1/processing_batches?id=eq.${batchId}`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
      apikey: ENV.SUPABASE_SERVICE_KEY,
      "Content-Type": "application/json",
      Prefer: "return=minimal",
    },
    body: JSON.stringify({
      recovery_attempted: true,
      last_recovery_attempt: new Date().toISOString(),
    }),
  });

  if (!response.ok) {
    logger.warn("Failed to mark batch for recovery", { batchId });
  }
}

// ============================================
// STORAGE OPERATIONS
// ============================================

async function listBatchFiles(
  batchId: string,
  clientId: string
): Promise<Array<{ name: string; path: string }>> {
  const prefix = `${clientId}/${batchId}/`;
  const encodedPrefix = encodeURIComponent(prefix);

  const response = await fetch(
    `${ENV.SUPABASE_URL}/storage/v1/object/list/${SUPABASE_CONFIG.STORAGE_BUCKET}?prefix=${encodedPrefix}`,
    {
      headers: {
        Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
        apikey: ENV.SUPABASE_SERVICE_KEY,
      },
    }
  );

  if (!response.ok) {
    throw new Error(`Failed to list batch files: ${response.status}`);
  }

  const files = await response.json();

  // Filter out directories and only include CV files (not in subfolders)
  return files
    .filter((f: any) => {
      const relativePath = f.name.replace(prefix, "");
      const isInSubfolder = relativePath.includes("/");
      return !isInSubfolder && VALIDATION_RULES.ALLOWED_EXTENSIONS.some(ext => f.name.toLowerCase().endsWith(ext));
    })
    .map((f: any) => ({
      name: f.name.split("/").pop(),
      path: f.name,
    }));
}

async function downloadFile(filePath: string): Promise<ArrayBuffer> {
  const encodedPath = encodeStoragePath(filePath);
  const downloadUrl = `${ENV.SUPABASE_URL}/storage/v1/object/${SUPABASE_CONFIG.STORAGE_BUCKET}/${encodedPath}`;

  const response = await fetch(downloadUrl, {
    headers: {
      Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
      apikey: ENV.SUPABASE_SERVICE_KEY,
    },
  });

  if (!response.ok) {
    throw new Error(`Failed to download file: ${response.status}`);
  }

  return await response.arrayBuffer();
}

async function getFileSize(filePath: string): Promise<number> {
  const encodedPath = encodeStoragePath(filePath);
  const infoUrl = `${ENV.SUPABASE_URL}/storage/v1/object/info/${SUPABASE_CONFIG.STORAGE_BUCKET}/${encodedPath}`;

  const response = await fetch(infoUrl, {
    headers: {
      Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
      apikey: ENV.SUPABASE_SERVICE_KEY,
    },
  });

  if (!response.ok) {
    return 0;
  }

  const info = await response.json();
  return info?.size || 0;
}