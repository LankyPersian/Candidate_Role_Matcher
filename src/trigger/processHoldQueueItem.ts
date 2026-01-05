// ============================================
// PROCESS HOLD QUEUE ITEM - PRODUCTION VERSION
// ============================================
// Handles manual review submissions with:
// - Cost tracking
// - Complete GHL field sync with file uploads
// - Comprehensive error handling
// - Duplicate resolution
// - Retry logic

import { task, logger } from "@trigger.dev/sdk";
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
} from "./config";
import {
  trackFullParse,
  trackGHLCall,
  logBatchCostSummary,
} from "./costTracker";
import { buildCompleteGHLCustomFields, splitName } from "./ghlTransformers";

// ============================================
// TYPES
// ============================================

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

// ============================================
// HELPER
// ============================================

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ============================================
// MAIN TASK
// ============================================

export const processHoldQueueItem = task({
  id: "process-hold-queue-item",
  maxDuration: 300, // 5 minutes
  run: async (payload: { holdQueueId: string }) => {
    const { holdQueueId } = payload;
    const correlationId = `hold_${holdQueueId}`;

    logger.info("🔄 Processing hold_queue item", { correlationId, holdQueueId });

    try {
      // 1) Fetch hold queue item
      const holdItem = await fetchHoldQueueItem(holdQueueId);

      if (!holdItem) {
        logger.error("Hold queue item not found", { correlationId, holdQueueId });
        return { success: false, error: "Item not found" };
      }

      if (holdItem.status !== "ready_for_processing") {
        logger.warn("Hold queue item not ready", {
          correlationId,
          holdQueueId,
          status: holdItem.status,
        });
        return { success: false, error: "Not ready for processing" };
      }

      logger.info("Hold queue item retrieved", {
        correlationId,
        holdQueueId,
        fileName: holdItem.file_name,
        reason: holdItem.extraction_data?.reason,
      });

      // 2) Parse CV data
      let parsedData: ParsedCV;

      if (holdItem.cv_raw_text && holdItem.cv_raw_text.trim().length > 50) {
        logger.info("Running full parse on raw text", { correlationId });
        parsedData = await fullParse(holdItem.cv_raw_text);
        parsedData.cv_raw_text = holdItem.cv_raw_text;
        await trackFullParse(holdItem.batch_id);
      } else {
        logger.warn("No raw text available, using empty CV", { correlationId });
        parsedData = getEmptyParsedCV();
      }

      // 3) Merge manual contact info (overrides parsed data)
      const manualInfo = holdItem.manual_contact_info || {};
      parsedData.full_name =
        manualInfo.full_name || parsedData.full_name || holdItem.extracted_name;
      parsedData.email = manualInfo.email || parsedData.email;
      parsedData.phone = manualInfo.phone || parsedData.phone;

      logger.info("Final candidate data", {
        correlationId,
        full_name: parsedData.full_name,
        email: parsedData.email,
        phone: parsedData.phone,
      });

      // 4) Determine if updating existing candidate
      let shouldUpdate = false;
      let existingCandidateId = null;

      if (holdItem.duplicate_candidate_id) {
        shouldUpdate = true;
        existingCandidateId = holdItem.duplicate_candidate_id;
        logger.info("Marked to update existing candidate", {
          correlationId,
          existingCandidateId,
        });
      } else if (!holdItem.ignore_duplicate) {
        const duplicate = await findExistingCandidate(parsedData.email, parsedData.phone);
        if (duplicate) {
          shouldUpdate = true;
          existingCandidateId = duplicate.id;
          logger.info("Found existing candidate", { correlationId, existingCandidateId });
        }
      }

      // 5) Check for GHL duplicate
      let ghlContactIdToUpdate = null;
      if (holdItem.extraction_data?.ghl_duplicate_contact_id) {
        ghlContactIdToUpdate = holdItem.extraction_data.ghl_duplicate_contact_id;
        logger.info("GHL duplicate detected - will update existing contact", {
          correlationId,
          ghlContactId: ghlContactIdToUpdate,
        });
      }

      // 6) Write/update candidate in Supabase
      let candidateId: string;

      if (shouldUpdate && existingCandidateId) {
        await updateCandidate(existingCandidateId, {
          ...parsedData,
          cv_file_path: holdItem.cv_file_path,
          batch_id: holdItem.batch_id,
          status: "pending_ghl_sync",
          is_update: true,
        });
        candidateId = existingCandidateId;
        logger.info("✅ Updated existing candidate in Supabase", {
          correlationId,
          candidateId,
        });
      } else {
        candidateId = await writeCandidate({
          ...parsedData,
          client_id: holdItem.client_id || null,
          ghl_contact_id: null,
          cv_file_path: holdItem.cv_file_path,
          batch_id: holdItem.batch_id,
          status: "pending_ghl_sync",
        });
        logger.info("✅ Created new candidate in Supabase", {
          correlationId,
          candidateId,
        });
      }

      // 7) Sync to GHL
      const ghlAccessToken = ENV.GHL_PRIVATE_KEY;
      let ghlContactId = null;

      try {
        // If we already have a GHL contact ID from duplicate detection, use it
        if (ghlContactIdToUpdate) {
          ghlContactId = ghlContactIdToUpdate;
        } else {
          // Otherwise, search for existing contact
          const existingContactId = await findExistingGHLContact(
            parsedData.email,
            parsedData.phone,
            ghlAccessToken
          );
          await trackGHLCall("search_contact", holdItem.batch_id);

          if (existingContactId) {
            ghlContactId = existingContactId;
            logger.info("Found existing GHL contact", {
              correlationId,
              ghlContactId,
            });
          } else {
            ghlContactId = await createGHLContact(parsedData, ghlAccessToken);
            await trackGHLCall("create_contact", holdItem.batch_id);
            logger.info("✅ Created new GHL contact", {
              correlationId,
              ghlContactId,
            });
          }
        }

        // Upload CV file to GHL
        let cvFileUrl: string | null = null;
        if (ghlContactId && holdItem.cv_file_path) {
          const cvUploadResult = await uploadCVToGHL(
            ghlContactId,
            holdItem.cv_file_path,
            holdItem.file_name || "cv.pdf",
            ghlAccessToken
          );
          await trackGHLCall("upload_file", holdItem.batch_id);

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
        }

        // Upload cover letter (if exists)
        let coverLetterUrl: string | null = null;
        if (ghlContactId && holdItem.cv_file_path) {
          try {
            coverLetterUrl = await uploadCoverLetterToGHL(
              ghlContactId,
              holdItem.cv_file_path,
              ghlAccessToken
            );
            if (coverLetterUrl) {
              await trackGHLCall("upload_file", holdItem.batch_id);
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
        }

        // Upload other documents (if exists)
        let otherDocsUrl: string | null = null;
        if (ghlContactId && holdItem.cv_file_path) {
          try {
            otherDocsUrl = await uploadOtherDocsToGHL(
              ghlContactId,
              holdItem.cv_file_path,
              ghlAccessToken
            );
            if (otherDocsUrl) {
              await trackGHLCall("upload_file", holdItem.batch_id);
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
        await trackGHLCall("update_contact", holdItem.batch_id);

        logger.info("✅ GHL contact updated with full data", {
          correlationId,
          ghlContactId,
          cvUploaded: !!cvFileUrl,
          coverLetterUploaded: !!coverLetterUrl,
          otherDocsUploaded: !!otherDocsUrl,
        });

        await updateCandidateGHL(candidateId, ghlContactId, "complete");
        logger.info("✅ GHL sync successful", {
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

        await updateCandidateGHL(candidateId, null, "ghl_sync_failed");
      }

      // 8) Mark hold queue item as complete
      await updateHoldQueueStatus(holdQueueId, "complete", candidateId);

      logger.info("✅ Hold queue item processing complete", {
        correlationId,
        holdQueueId,
        candidateId,
        ghlContactId,
      });

      return {
        success: true,
        candidateId,
        ghlContactId,
      };
    } catch (error: any) {
      logger.error("❌ Hold queue processing failed", {
        correlationId,
        holdQueueId,
        error: error.message,
        stack: error.stack,
      });

      // Mark as failed but don't delete (allow retry)
      await updateHoldQueueStatus(holdQueueId, "pending", null, error.message);

      throw error;
    }
  },
});

// ============================================
// GEMINI API - FULL PARSE
// ============================================

function stripJsonFences(s: string): string {
  let t = (s || "").trim();
  if (t.startsWith("```")) {
    t = t.replace(/^```[a-zA-Z]*\n?/, "").replace(/```$/, "").trim();
  }
  return t;
}

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
      outputPreview: cleaned.slice(0, 1000),
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
): Promise<{ id: string } | null> {
  if (email && isValidEmail(email)) {
    const response = await fetch(
      `${ENV.SUPABASE_URL}/rest/v1/candidates?email=eq.${encodeURIComponent(email)}&select=id&limit=1`,
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
        `${ENV.SUPABASE_URL}/rest/v1/candidates?phone=eq.${encodeURIComponent(normalized)}&select=id&limit=1`,
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

// ============================================
// GHL OPERATIONS
// ============================================

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
  candidateId: string,
  cvFileUrl: string | null,
  coverLetterUrl: string | null,
  otherDocsUrl: string | null,
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
        `GHL upload failed: ${uploadResponse.status} - ${errorText.slice(0, 800)}`
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
          return uploadResult?.url || uploadResult?.fileUrl || uploadResult?.publicUrl || null;
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
          return uploadResult?.url || uploadResult?.fileUrl || uploadResult?.publicUrl || null;
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

async function fetchHoldQueueItem(holdQueueId: string): Promise<any> {
  const response = await fetch(
    `${ENV.SUPABASE_URL}/rest/v1/hold_queue?id=eq.${holdQueueId}&select=*&limit=1`,
    {
      headers: {
        Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
        apikey: ENV.SUPABASE_SERVICE_KEY,
      },
    }
  );

  if (!response.ok) {
    throw new Error(`Failed to fetch hold queue item: ${response.status}`);
  }

  const data = await response.json();
  return data[0] || null;
}

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

async function updateCandidate(candidateId: string, updateData: any): Promise<void> {
  const response = await fetch(`${ENV.SUPABASE_URL}/rest/v1/candidates?id=eq.${candidateId}`, {
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
    throw new Error(`Failed to update candidate: ${t.slice(0, 800)}`);
  }
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

async function updateHoldQueueStatus(
  holdQueueId: string,
  status: string,
  candidateId: string | null,
  errorMessage?: string
): Promise<void> {
  const updateData: any = {
    status,
    processed_at: new Date().toISOString(),
  };

  if (candidateId) {
    updateData.existing_candidate_id = candidateId;
  }

  if (errorMessage) {
    updateData.processing_error = errorMessage;
  }

  const response = await fetch(`${ENV.SUPABASE_URL}/rest/v1/hold_queue?id=eq.${holdQueueId}`, {
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
    throw new Error(`Failed to update hold queue status: ${t.slice(0, 800)}`);
  }
}