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
import { mapFieldsToGHLFormat } from "./ghlFieldMapper";
import { PACK_GROUPING_CONFIG } from "./config";

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
  cv_raw_text?: string;
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
// FILE GROUPING & PACK PROCESSING
// ============================================

interface ProcessedFile {
  name: string;
  path: string;
  rawText: string;
  classification: ClassificationResult;
  quickData?: Partial<ParsedCV> & { is_student?: boolean };
  documentType: "cv" | "cover_letter" | "application" | "supporting_document";
  packId?: string;
}

interface CandidatePack {
  packId: string;
  identityKey: string; // email (normalized) or phone (normalized) or name (normalized)
  files: ProcessedFile[];
  quickData: Partial<ParsedCV> & { is_student?: boolean };
  combinedRawText: string;
  documents: Array<{
    type: string;
    path: string;
    name: string;
    document_type: string;
  }>;
}

/**
 * Normalize name for grouping (lowercase, remove special chars)
 */
function normalizeNameForGrouping(name: string | null): string | null {
  if (!name) return null;
  return name
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9\s]/g, "")
    .replace(/\s+/g, " ");
}

/**
 * Group files into candidate packs based on identity extraction
 */
function groupFilesIntoPacks(files: ProcessedFile[]): CandidatePack[] {
  const packMap = new Map<string, CandidatePack>();
  const orphanedFiles: ProcessedFile[] = [];

  // First pass: Group by email/phone (strongest identifiers)
  for (const file of files) {
    if (!file.quickData) continue;

    const email = file.quickData.email && isValidEmail(file.quickData.email)
      ? file.quickData.email.toLowerCase().trim()
      : null;
    const phone = file.quickData.phone ? normalizePhone(file.quickData.phone) : null;
    const name = normalizeNameForGrouping(file.quickData.full_name);

    // Determine identity key (email > phone > name)
    let identityKey: string | null = null;
    if (email && PACK_GROUPING_CONFIG.GROUP_BY_EMAIL) {
      identityKey = `email:${email}`;
    } else if (phone && PACK_GROUPING_CONFIG.GROUP_BY_PHONE) {
      identityKey = `phone:${phone}`;
    } else if (name && PACK_GROUPING_CONFIG.GROUP_BY_NAME) {
      identityKey = `name:${name}`;
    }

    if (!identityKey) {
      orphanedFiles.push(file);
      continue;
    }

    // Get or create pack
    if (!packMap.has(identityKey)) {
      const packId = `pack_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
      packMap.set(identityKey, {
        packId,
        identityKey,
        files: [],
        quickData: file.quickData,
        combinedRawText: "",
        documents: [],
      });
    }

    const pack = packMap.get(identityKey)!;
    
    // Merge quickData (prefer non-null values)
    if (file.quickData.email && !pack.quickData.email) pack.quickData.email = file.quickData.email;
    if (file.quickData.phone && !pack.quickData.phone) pack.quickData.phone = file.quickData.phone;
    if (file.quickData.full_name && !pack.quickData.full_name) pack.quickData.full_name = file.quickData.full_name;
    if (file.quickData.is_student !== undefined) pack.quickData.is_student = file.quickData.is_student;
    
    pack.files.push(file);
    file.packId = pack.packId;
  }

  // Second pass: Try to match orphaned files by name patterns
  if (PACK_GROUPING_CONFIG.GROUP_BY_NAME && orphanedFiles.length > 0) {
    for (const orphan of orphanedFiles) {
      const orphanName = normalizeNameForGrouping(orphan.quickData?.full_name || null);
      if (!orphanName) continue;

      // Try to match with existing packs by name
      let matched = false;
      for (const [key, pack] of packMap.entries()) {
        if (key.startsWith("name:")) {
          const packName = normalizeNameForGrouping(pack.quickData.full_name || null);
          if (packName && orphanName === packName) {
            pack.files.push(orphan);
            orphan.packId = pack.packId;
            matched = true;
            break;
          }
        }
      }

      // If no match and allow single-file packs, create new pack
      if (!matched && PACK_GROUPING_CONFIG.ALLOW_SINGLE_FILE_PACKS) {
        const orphanPackId = `pack_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
        const identityKey = `name:${orphanName}`;
        packMap.set(identityKey, {
          packId: orphanPackId,
          identityKey,
          files: [orphan],
          quickData: orphan.quickData || {},
          combinedRawText: "",
          documents: [],
        });
        orphan.packId = orphanPackId;
      }
    }
  }

  // Build documents metadata and combine raw text for each pack
  const packs: CandidatePack[] = [];
  for (const pack of packMap.values()) {
    // Sort files: CV first, then cover letter, then application, then supporting
    const fileOrder: Record<string, number> = {
      cv: 0,
      resume: 0,
      cover_letter: 1,
      application: 2,
      supporting_document: 3,
    };
    
    pack.files.sort((a, b) => {
      const aOrder = fileOrder[a.documentType] ?? 99;
      const bOrder = fileOrder[b.documentType] ?? 99;
      return aOrder - bOrder;
    });

    // Build documents metadata
    pack.documents = pack.files.map((file) => ({
      type: file.documentType,
      path: file.path,
      name: file.name,
      document_type: file.classification.document_type,
    }));

    // Combine raw text with separators
    const textParts: string[] = [];
    for (const file of pack.files) {
      if (file.rawText && file.rawText.trim()) {
        textParts.push(`--- ${file.name} (${file.documentType}) ---\n${file.rawText.trim()}\n`);
      }
    }
    pack.combinedRawText = textParts.join("\n\n");

    // Safety check: max files per pack
    if (pack.files.length <= PACK_GROUPING_CONFIG.MAX_FILES_PER_PACK) {
      packs.push(pack);
    } else {
      logger.warn("Pack exceeds max files, splitting", {
        packId: pack.packId,
        fileCount: pack.files.length,
        maxFiles: PACK_GROUPING_CONFIG.MAX_FILES_PER_PACK,
      });
      // Split into multiple packs (take first MAX_FILES_PER_PACK)
      const splitPack = {
        ...pack,
        files: pack.files.slice(0, PACK_GROUPING_CONFIG.MAX_FILES_PER_PACK),
      };
      splitPack.documents = splitPack.files.map((file) => ({
        type: file.documentType,
        path: file.path,
        name: file.name,
        document_type: file.classification.document_type,
      }));
      const splitTextParts: string[] = [];
      for (const file of splitPack.files) {
        if (file.rawText && file.rawText.trim()) {
          splitTextParts.push(`--- ${file.name} (${file.documentType}) ---\n${file.rawText.trim()}\n`);
        }
      }
      splitPack.combinedRawText = splitTextParts.join("\n\n");
      packs.push(splitPack);
    }
  }

  return packs;
}

/**
 * Determine document type from classification and filename
 */
function determineDocumentType(
  classification: ClassificationResult,
  fileName: string
): "cv" | "cover_letter" | "application" | "supporting_document" {
  const lowerName = fileName.toLowerCase();

  // Check filename patterns first
  if (lowerName.includes("cover") || lowerName.includes("letter") || classification.document_type === "cover_letter") {
    return "cover_letter";
  }
  if (lowerName.includes("application") || lowerName.includes("app") || classification.document_type === "application") {
    return "application";
  }
  if (classification.document_type === "cv" || classification.document_type === "resume") {
    return "cv";
  }

  // Default to supporting_document
  return "supporting_document";
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

      // ============================================
      // PHASE 1: Extract, classify, and quick parse all files
      // ============================================
      logger.info("📋 Phase 1: Extracting text and classifying documents", { batchId });

      const processedFiles: ProcessedFile[] = [];

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

          // Mark file as processing (document_type will be set after classification)
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

          // Validate text length (relaxed for multi-doc support)
          const minTextLength = Math.min(PROCESSING_CONFIG.MIN_TEXT_LENGTH_REQUIRED, 30); // Allow shorter docs
          if (!rawText || rawText.trim().length < minTextLength) {
            logger.warn("❌ File has insufficient text - rejecting", {
              correlationId,
              fileName: file.name,
              textLength: rawText?.trim().length || 0,
              required: minTextLength,
            });

            await updateFileStatus(
              batchId,
              file.path,
              file.name,
              "failed",
              "Insufficient text content"
            );
            const rejectionClassification: ClassificationResult = {
              document_type: "irrelevant",
              confidence: 1.0,
              reasoning: "Insufficient text content",
              should_process: false,
              key_indicators: ["insufficient_text"],
              rejection_reason: "Text too short",
            };
            await recordRejection(batchId, file.name, file.path, rejectionClassification);
            stats.failed++;
            continue;
          }

          // 🔥 DOCUMENT CLASSIFICATION
          logger.info("🔍 Classifying document", {
            correlationId,
            fileName: file.name,
          });

          // Always classify (no heuristic pre-filter for multi-doc support)
          const classification = await classifyDocument(rawText, file.name, batchId);
          stats.classified++;
          classificationResults.push(classification);

          // Reject only if truly irrelevant
          if (!classification.should_process || classification.document_type === "irrelevant") {
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
              classification.rejection_reason || "Document irrelevant"
            );

            continue;
          }

          logger.info("✅ Document accepted for processing", {
            correlationId,
            file: file.name,
            type: classification.document_type,
            confidence: `${(classification.confidence * 100).toFixed(1)}%`,
          });

          // Determine document type
          const documentType = determineDocumentType(classification, file.name);

          // Quick parse for contact info (only for CV/resume to extract identity)
          let quickData: (Partial<ParsedCV> & { is_student?: boolean }) | undefined;
          if (documentType === "cv") {
            quickData = await quickParse(rawText);
            await trackQuickParse(batchId);

            logger.info("Quick parse extracted", {
              correlationId,
              email: quickData.email || "none",
              phone: quickData.phone || "none",
              name: quickData.full_name || "none",
            });
          } else {
            // For non-CV documents, try minimal identity extraction
            const emailMatch = rawText.match(/[\w.-]+@[\w.-]+\.\w+/);
            const phoneMatch = rawText.match(/(\+?\d[\d\s().-]{8,}\d)/);
            quickData = {
              email: emailMatch ? emailMatch[0] : null,
              phone: phoneMatch ? phoneMatch[0] : null,
            };
          }

          // Store processed file
          processedFiles.push({
            name: file.name,
            path: file.path,
            rawText,
            classification,
            quickData,
            documentType,
          });

          // File status updated (document_type stored in processedFiles array, will be written after pack grouping)

        } catch (fileError: any) {
          logger.error("❌ File processing exception in Phase 1", {
            correlationId,
            fileName: file.name,
            error: fileError.message,
            stack: fileError.stack,
          });

          stats.failed++;

          await updateFileStatus(
            batchId,
            file.path,
            file.name,
            "failed",
            fileError.message
          );

          if (PROCESSING_CONFIG.CONTINUE_ON_FILE_ERROR) {
            continue;
          } else {
            throw fileError;
          }
        }
      }

      logger.info("✅ Phase 1 complete", {
        batchId,
        totalFiles: files.length,
        processedFiles: processedFiles.length,
        rejectedFiles: stats.rejected_by_classification,
        failedFiles: stats.failed,
      });

      // ============================================
      // PHASE 2: Group files into candidate packs
      // ============================================
      logger.info("📦 Phase 2: Grouping files into candidate packs", { batchId });

      const packs = groupFilesIntoPacks(processedFiles);

      logger.info("✅ File grouping complete", {
        batchId,
        totalPacks: packs.length,
        totalFiles: processedFiles.length,
        avgFilesPerPack: packs.length > 0 ? (processedFiles.length / packs.length).toFixed(1) : "0",
      });

      // ============================================
      // PHASE 3: Process each candidate pack
      // ============================================
      logger.info("🔄 Phase 3: Processing candidate packs", { batchId, packCount: packs.length });

      // Update all file statuses with pack_id and document_type
      for (const pack of packs) {
        for (const file of pack.files) {
          await updateFileStatus(
            batchId,
            file.path,
            file.name,
            "processing",
            undefined,
            undefined,
            undefined,
            file.documentType,
            pack.packId
          );
        }
      }

      // Process each pack
      for (let packIndex = 0; packIndex < packs.length; packIndex++) {
        const pack = packs[packIndex];
        const correlationId = `pack_${pack.packId}`;

        try {
          logger.info(`📦 Processing pack ${packIndex + 1}/${packs.length}`, {
            correlationId,
            packId: pack.packId,
            fileCount: pack.files.length,
            identityKey: pack.identityKey,
          });

          // Find CV file in pack (required for processing)
          const cvFile = pack.files.find(f => f.documentType === "cv");
          if (!cvFile) {
            logger.warn("⚠️ Pack missing CV file - sending to hold queue", {
              correlationId,
              packId: pack.packId,
            });

            stats.held_for_review++;
            await addPackToHoldQueue(batchId, clientId, pack, {
              reason: "missing_cv_file",
            });

            // Mark all files as complete (sent to hold queue)
            for (const file of pack.files) {
              await updateFileStatus(
                batchId,
                file.path,
                file.name,
                "complete",
                undefined,
                undefined,
                "Sent to hold queue - missing CV file",
                file.documentType,
                pack.packId
              );
            }

            continue;
          }

          // Merge quick data from all files (prioritize CV data)
          const mergedQuickData = cvFile.quickData || pack.quickData || {};
          
          // Merge skills from all files
          const allSkills = new Set<string>();
          for (const file of pack.files) {
            if (file.quickData?.skills && Array.isArray(file.quickData.skills)) {
              file.quickData.skills.forEach((s: string) => allSkills.add(s));
            }
          }
          mergedQuickData.skills = Array.from(allSkills);

          // PRE-QUALIFICATION FILTERING
          // Check student status
          if (batchConfig.exclude_students && mergedQuickData.is_student === true) {
            logger.warn("❌ Pack rejected by filter - student status", {
              correlationId,
              packId: pack.packId,
            });

            stats.rejected_by_filters++;
            for (const file of pack.files) {
              await recordFilterRejection(batchId, file.name, file.path, {
                reason: "Currently enrolled as student (excluded by filter)",
                is_student: true,
                pack_id: pack.packId,
              });

              await updateFileStatus(
                batchId,
                file.path,
                file.name,
                "rejected",
                "Currently enrolled as student",
                undefined,
                undefined,
                file.documentType,
                pack.packId
              );
            }

            continue;
          }

          // Check required skills
          if (batchConfig.required_skills.length > 0) {
            const candidateSkills = mergedQuickData.skills || [];
            const hasRequiredSkills = batchConfig.required_skills.some(requiredSkill =>
              candidateSkills.some((skill: string) =>
                skill.toLowerCase().includes(requiredSkill.toLowerCase())
              )
            );

            if (!hasRequiredSkills) {
              const missingSkills = batchConfig.required_skills.filter(req =>
                !candidateSkills.some((cs: string) => cs.toLowerCase().includes(req.toLowerCase()))
              );

              logger.warn("❌ Pack rejected by filter - missing required skills", {
                correlationId,
                packId: pack.packId,
                required: batchConfig.required_skills,
                found: candidateSkills,
                missing: missingSkills,
              });

              stats.rejected_by_filters++;
              for (const file of pack.files) {
                await recordFilterRejection(batchId, file.name, file.path, {
                  reason: `Missing required skills: ${missingSkills.join(', ')}`,
                  required_skills: batchConfig.required_skills,
                  candidate_skills: candidateSkills,
                  missing_skills: missingSkills,
                  pack_id: pack.packId,
                });

                await updateFileStatus(
                  batchId,
                  file.path,
                  file.name,
                  "rejected",
                  `Missing required skills: ${missingSkills.join(', ')}`,
                  undefined,
                  undefined,
                  file.documentType,
                  pack.packId
                );
              }

              continue;
            }
          }

          // Check for required contact info
          const hasEmail = mergedQuickData.email && isValidEmail(mergedQuickData.email);
          const hasPhone = mergedQuickData.phone && normalizePhone(mergedQuickData.phone);

          if (PROCESSING_CONFIG.REQUIRE_EMAIL_OR_PHONE && !hasEmail && !hasPhone) {
            logger.warn("⏸️ Pack missing contact info - sending to hold queue", {
              correlationId,
              packId: pack.packId,
            });

            stats.held_for_review++;
            await addPackToHoldQueue(batchId, clientId, pack, {
              reason: "missing_contact_info",
            });

            // Mark all files as complete (sent to hold queue)
            for (const file of pack.files) {
              await updateFileStatus(
                batchId,
                file.path,
                file.name,
                "complete",
                undefined,
                undefined,
                "Sent to hold queue - missing contact info",
                file.documentType,
                pack.packId
              );
            }

            continue;
          }

          // Duplicate detection
          const existingCandidate = await findExistingCandidate(
            mergedQuickData.email ?? null,
            mergedQuickData.phone ?? null
          );
          const ghlDuplicate = await findExistingGHLContact(
            mergedQuickData.email ?? null,
            mergedQuickData.phone ?? null,
            ghlAccessToken
          );
          await trackGHLCall("search_contact", batchId);

          if (existingCandidate || ghlDuplicate) {
            logger.warn("🔍 Duplicate detected for pack", {
              correlationId,
              packId: pack.packId,
              supabaseDuplicate: !!existingCandidate,
              ghlDuplicate: !!ghlDuplicate,
            });

            stats.duplicates_found++;
            stats.held_for_review++;

            await addPackToHoldQueue(batchId, clientId, pack, {
              reason: "duplicate_detected",
              supabase_duplicate_id: existingCandidate?.id,
              ghl_duplicate_contact_id: ghlDuplicate,
            });

            // Mark all files as complete (sent to hold queue)
            for (const file of pack.files) {
              await updateFileStatus(
                batchId,
                file.path,
                file.name,
                "complete",
                undefined,
                undefined,
                "Sent to hold queue - duplicate detected",
                file.documentType,
                pack.packId
              );
            }

            continue;
          }

          // Full parse on combined pack text
          logger.info("🔍 Parsing combined pack text", {
            correlationId,
            packId: pack.packId,
            combinedTextLength: pack.combinedRawText.length,
            fileCount: pack.files.length,
          });

          const parsedData = await fullParse(pack.combinedRawText);
          parsedData.documents_raw_text = pack.combinedRawText; // Store combined text
          parsedData.cv_raw_text = pack.combinedRawText; // Backward compatibility
          await trackFullParse(batchId);

          // Merge quick parse contact info (in case full parse missed it)
          parsedData.email = parsedData.email || (mergedQuickData.email ?? null);
          parsedData.phone = parsedData.phone || (mergedQuickData.phone ?? null);
          parsedData.full_name = parsedData.full_name || (mergedQuickData.full_name ?? null);

          logger.info("Full parse complete for pack", {
            correlationId,
            packId: pack.packId,
            fieldsExtracted: Object.keys(parsedData).filter(k => parsedData[k as keyof ParsedCV]).length,
          });

          // Identify CV, cover letter, and other docs from pack
          const cvFileDoc = pack.files.find(f => f.documentType === "cv");
          const coverLetterFiles = pack.files.filter(f => f.documentType === "cover_letter");
          const otherDocsFiles = pack.files.filter(f => 
            f.documentType === "application" || f.documentType === "supporting_document"
          );

          // Write to Supabase with documents metadata
          const candidateId = await writeCandidate({
            ...parsedData,
            client_id: clientId || null,
            ghl_contact_id: null,
            cv_file_path: cvFileDoc?.path || null,
            cover_letter_file_path: coverLetterFiles.length > 0 ? coverLetterFiles[0].path : null,
            application_docs_file_paths: otherDocsFiles.length > 0 
              ? otherDocsFiles.map(f => f.path)
              : null,
            documents: pack.documents,
            documents_raw_text: pack.combinedRawText,
            batch_id: batchId,
            status: "pending_ghl_sync",
          });

          logger.info("✅ Candidate saved to Supabase with pack documents", {
            correlationId,
            candidateId,
            packId: pack.packId,
            cvFile: !!cvFileDoc,
            coverLetters: coverLetterFiles.length,
            otherDocs: otherDocsFiles.length,
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

            // Upload CV file to GHL (candidate_provided_cv field)
            let cvFileUrl: string | null = null;
            if (cvFileDoc) {
              try {
                const cvUploadResult = await uploadFileToGHL(
                  ghlContactId,
                  cvFileDoc.path,
                  cvFileDoc.name,
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
            }

            // Upload cover letter (cover_letter field) - take first cover letter
            let coverLetterUrl: string | null = null;
            if (coverLetterFiles.length > 0) {
              try {
                const clFile = coverLetterFiles[0];
                const clUploadResult = await uploadFileToGHL(
                  ghlContactId,
                  clFile.path,
                  clFile.name,
                  ghlAccessToken
                );
                if (clUploadResult.success && clUploadResult.fileUrl) {
                  await trackGHLCall("upload_file", batchId);
                  coverLetterUrl = clUploadResult.fileUrl;
                  logger.info("✅ Cover letter uploaded to GHL", {
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
            }

            // Upload other documents (application__other_documents field) - combine if multiple
            let otherDocsUrl: string | null = null;
            if (otherDocsFiles.length > 0) {
              // For now, upload first other doc (can be enhanced to handle multiple)
              try {
                const otherDocFile = otherDocsFiles[0];
                const odUploadResult = await uploadFileToGHL(
                  ghlContactId,
                  otherDocFile.path,
                  otherDocFile.name,
                  ghlAccessToken
                );
                if (odUploadResult.success && odUploadResult.fileUrl) {
                  await trackGHLCall("upload_file", batchId);
                  otherDocsUrl = odUploadResult.fileUrl;
                  logger.info("✅ Other documents uploaded to GHL", {
                    correlationId,
                    ghlContactId,
                    fileUrl: otherDocsUrl,
                    totalOtherDocs: otherDocsFiles.length,
                  });
                }
              } catch (odError: any) {
                logger.warn("⚠️ Other documents upload failed", {
                  correlationId,
                  error: odError.message,
                });
              }
            }

            // Update GHL contact with all fields + file URLs using robust field mapping
            await updateGHLContactWithMapping(
              ghlContactId,
              parsedData,
              candidateId,
              cvFileUrl,
              coverLetterUrl,
              otherDocsUrl,
              ghlAccessToken
            );
            await trackGHLCall("update_contact", batchId);

            logger.info("✅ GHL contact updated with full pack data", {
              correlationId,
              ghlContactId,
              cvUploaded: !!cvFileUrl,
              coverLetterUploaded: !!coverLetterUrl,
              otherDocsUploaded: !!otherDocsUrl,
            });

            // Update candidate with GHL contact ID
            await updateCandidateGHL(candidateId, ghlContactId, "complete");

            stats.processed++;

            // Mark all files in pack as complete
            for (const file of pack.files) {
              await updateFileStatus(
                batchId,
                file.path,
                file.name,
                "complete",
                undefined,
                candidateId,
                undefined,
                file.documentType,
                pack.packId
              );
            }

            logger.info("✅ Pack processing complete", {
              correlationId,
              packId: pack.packId,
              candidateId,
              ghlContactId,
              fileCount: pack.files.length,
            });

          } catch (ghlError: any) {
            logger.error("❌ GHL sync failed for pack", {
              correlationId,
              packId: pack.packId,
              error: ghlError.message,
              stack: ghlError.stack,
              candidateId,
            });

            // Mark candidate as GHL sync failed (but data is safe in Supabase)
            await updateCandidateGHL(candidateId, null, "ghl_sync_failed");

            // Mark all files as complete with warning
            for (const file of pack.files) {
              await updateFileStatus(
                batchId,
                file.path,
                file.name,
                "complete",
                `GHL sync failed: ${ghlError.message}`,
                candidateId,
                undefined,
                file.documentType,
                pack.packId
              );
            }

            stats.processed++; // Still count as processed (saved to Supabase)
          }

          // Small delay between packs
          if (packIndex < packs.length - 1) {
            await delay(PROCESSING_CONFIG.DELAY_BETWEEN_FILES_MS);
          }

          // Update batch progress (count processed packs)
          const processedPackCount = packIndex + 1;
          await updateBatchProgress(batchId, processedPackCount, packs.length);

        } catch (packError: any) {
          logger.error("❌ Pack processing exception", {
            correlationId,
            packId: pack.packId,
            error: packError.message,
            stack: packError.stack,
          });

          stats.failed++;

          // Mark all files in pack as failed
          for (const file of pack.files) {
            await updateFileStatus(
              batchId,
              file.path,
              file.name,
              "failed",
              packError.message,
              undefined,
              undefined,
              file.documentType,
              pack.packId
            );
          }

          // Continue to next pack (don't fail entire batch)
          if (PROCESSING_CONFIG.CONTINUE_ON_FILE_ERROR) {
            continue;
          } else {
            throw packError;
          }
        }
      }

      // Log classification stats
      if (classificationResults.length > 0) {
        logClassificationStats(classificationResults, batchId);
      }

      // Final batch status - count processed PACKS (not files)
      const totalProcessedPacks = stats.processed + stats.held_for_review + stats.failed + stats.rejected_by_filters;
      const finalStatus = stats.held_for_review > 0 ? "awaiting_input" : "complete";
      await updateBatchStatus(batchId, finalStatus, totalProcessedPacks);

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

/**
 * Update GHL contact with robust field mapping (key -> ID)
 */
async function updateGHLContactWithMapping(
  contactId: string,
  data: ParsedCV,
  candidateId: string | undefined,
  cvFileUrl: string | null | undefined,
  coverLetterUrl: string | null | undefined,
  otherDocsUrl: string | null | undefined,
  accessToken: string
): Promise<void> {
  // Build custom fields data (key-value pairs)
  const customFieldsData = buildCompleteGHLCustomFields(
    data,
    candidateId,
    cvFileUrl,
    coverLetterUrl,
    otherDocsUrl
  );

  // Map fields to GHL format (key -> ID if available)
  const mappedFields = await mapFieldsToGHLFormat(customFieldsData, accessToken);

  logger.info("Updating GHL contact with mapped fields", {
    contactId,
    fieldCount: mappedFields.length,
    usingIds: mappedFields.filter(f => f.id).length,
    usingKeys: mappedFields.filter(f => f.key).length,
  });

  const response = await fetch(`${GHL_CONFIG.BASE_URL}/contacts/${contactId}`, {
    method: "PUT",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json",
      Version: GHL_CONFIG.API_VERSION,
    },
    body: JSON.stringify({ customFields: mappedFields }),
  });

  if (!response.ok) {
    const txt = await response.text();
    throw new Error(`GHL contact update failed: ${txt.slice(0, 800)}`);
  }
}

/**
 * Legacy updateGHLContact (backward compatibility)
 */
async function updateGHLContact(
  contactId: string,
  data: ParsedCV,
  candidateId: string | undefined,
  cvFileUrl: string | null | undefined,
  coverLetterUrl: string | null | undefined,
  otherDocsUrl: string | null | undefined,
  accessToken: string
): Promise<void> {
  return updateGHLContactWithMapping(
    contactId,
    data,
    candidateId,
    cvFileUrl,
    coverLetterUrl,
    otherDocsUrl,
    accessToken
  );
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
 * Generic file upload to GHL - works for CV, cover letter, or other documents
 */
async function uploadFileToGHL(
  contactId: string,
  filePath: string,
  originalFilename: string,
  accessToken: string
): Promise<{ success: boolean; fileUrl?: string; error?: string }> {
  try {
    const encodedPath = encodeStoragePath(filePath);
    const downloadUrl = `${ENV.SUPABASE_URL}/storage/v1/object/${SUPABASE_CONFIG.STORAGE_BUCKET}/${encodedPath}`;

    const downloadResponse = await fetch(downloadUrl, {
      headers: {
        Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
        apikey: ENV.SUPABASE_SERVICE_KEY,
      },
    });

    if (!downloadResponse.ok) {
      const t = await downloadResponse.text();
      throw new Error(`Failed to download file: ${downloadResponse.status} - ${t.slice(0, 300)}`);
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
        `GHL file upload failed: ${uploadResponse.status} - ${errorText.slice(0, 800)}`
      );
    }

    const uploadResult = await uploadResponse.json();
    const fileUrl = uploadResult?.url || uploadResult?.fileUrl || uploadResult?.publicUrl || null;

    return {
      success: true,
      fileUrl,
    };
  } catch (error: any) {
    logger.error("Failed to upload file to GHL", {
      contactId,
      filePath,
      error: error?.message ?? String(error),
    });

    return {
      success: false,
      error: error?.message ?? String(error),
    };
  }
}

/**
 * Upload CV file to GHL (backward compatibility wrapper)
 */
async function uploadCVToGHL(
  contactId: string,
  cvFilePath: string,
  originalFilename: string,
  accessToken: string
): Promise<{ success: boolean; fileUrl?: string; error?: string }> {
  return uploadFileToGHL(contactId, cvFilePath, originalFilename, accessToken);
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
  processedCount: number,
  totalCount: number
): Promise<void> {
  const response = await fetch(`${ENV.SUPABASE_URL}/rest/v1/processing_batches?id=eq.${batchId}`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
      apikey: ENV.SUPABASE_SERVICE_KEY,
      "Content-Type": "application/json",
      Prefer: "return=minimal",
    },
    body: JSON.stringify({ 
      processed_count: processedCount,
      // Optionally store total for progress calculation
    }),
  });

  if (!response.ok) {
    const t = await response.text();
    throw new Error(`Failed to update batch progress: ${response.status} ${t.slice(0, 800)}`);
  }
}

async function addPackToHoldQueue(
  batchId: string,
  clientId: string,
  pack: CandidatePack,
  extraData: {
    reason: string;
    supabase_duplicate_id?: string;
    ghl_duplicate_contact_id?: string;
  }
): Promise<string> {
  // Build documents array for hold_queue
  const documents = pack.files.map((file) => ({
    type: file.documentType,
    path: file.path,
    name: file.name,
    document_type: file.classification.document_type,
    extracted_text_preview: file.rawText.substring(0, 500), // Preview for UI
  }));

  // Find CV file for cv_file_path (backward compatibility)
  const cvFile = pack.files.find(f => f.documentType === "cv");
  const extractedName = pack.quickData.full_name || cvFile?.quickData?.full_name || null;

  const record = {
    batch_id: batchId,
    client_id: clientId,
    extracted_name: extractedName,
    cv_file_path: cvFile?.path || pack.files[0]?.path || null, // Primary CV path for backward compat
    cv_raw_text: pack.combinedRawText.substring(0, 50000), // Store preview in cv_raw_text for backward compat
    documents_raw_text: pack.combinedRawText, // Full combined text
    documents: documents, // JSONB array of all documents
    status: "pending",
    extraction_data: {
      ...pack.quickData,
      reason: extraData.reason,
      supabase_duplicate_id: extraData.supabase_duplicate_id,
      ghl_duplicate_contact_id: extraData.ghl_duplicate_contact_id,
      pack_id: pack.packId,
      identity_key: pack.identityKey,
    },
    file_name: cvFile?.name || pack.files[0]?.name || "unknown", // Primary file name for backward compat
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
    throw new Error(`Failed to add pack to hold queue: ${errorText.slice(0, 800)}`);
  }

  const result = await response.json();
  const insertedId = result?.[0]?.id;

  logger.info("Added pack to hold_queue", {
    id: insertedId,
    packId: pack.packId,
    fileCount: pack.files.length,
    batchId,
    reason: extraData.reason,
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
  notes?: string,
  documentType?: string,
  packId?: string
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
  if (documentType) record.document_type = documentType;
  if (packId) record.pack_id = packId;
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
    `${ENV.SUPABASE_URL}/storage/v1/object/list/${SUPABASE_CONFIG.STORAGE_BUCKET}?prefix=${encodedPrefix}&limit=1000`,
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

  // Include ALL files (root level and subfolders) - they will be grouped into packs
  // Filter only by allowed extensions
  return files
    .filter((f: any) => {
      // Exclude directories (files with no extension or ending in /)
      if (!f.name || f.name.endsWith("/")) return false;
      
      // Check if has allowed extension
      return VALIDATION_RULES.ALLOWED_EXTENSIONS.some(ext => 
        f.name.toLowerCase().endsWith(ext.toLowerCase())
      );
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