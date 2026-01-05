// ============================================
// PROCESS HOLD QUEUE ITEM - PRODUCTION VERSION
// ============================================
// Handles manual review submissions with:
// - Cost tracking
// - Complete GHL field sync
// - Comprehensive error handling
// - Duplicate resolution

import { task, logger } from "@trigger.dev/sdk";
import {
  ENV,
  PROCESSING_CONFIG,
  SUPABASE_CONFIG,    // ← ADD THIS LINE
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

    logger.info("🔄 Processing hold_queue item", { holdQueueId });

    try {
      // 1) Fetch hold queue item
      const holdItem = await fetchHoldQueueItem(holdQueueId);

      if (!holdItem) {
        logger.error("Hold queue item not found", { holdQueueId });
        return { success: false, error: "Item not found" };
      }

      if (holdItem.status !== "ready_for_processing") {
        logger.warn("Hold queue item not ready", {
          holdQueueId,
          status: holdItem.status,
        });
        return { success: false, error: "Not ready for processing" };
      }

      logger.info("Hold queue item retrieved", {
        holdQueueId,
        fileName: holdItem.file_name,
        reason: holdItem.extraction_data?.reason,
      });

      // 2) Parse CV data
      let parsedData: ParsedCV;

      if (holdItem.cv_raw_text && holdItem.cv_raw_text.trim().length > 50) {
        logger.info("Running full parse on raw text");
        parsedData = await fullParse(holdItem.cv_raw_text);
        await trackFullParse(holdItem.batch_id);
      } else {
        logger.warn("No raw text available, using empty CV");
        parsedData = getEmptyParsedCV();
      }

      // 3) Merge manual contact info (overrides parsed data)
      const manualInfo = holdItem.manual_contact_info || {};
      parsedData.full_name =
        manualInfo.full_name || parsedData.full_name || holdItem.extracted_name;
      parsedData.email = manualInfo.email || parsedData.email;
      parsedData.phone = manualInfo.phone || parsedData.phone;

      logger.info("Final candidate data", {
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
          existingCandidateId,
        });
      } else if (!holdItem.ignore_duplicate) {
        const duplicate = await findExistingCandidate(parsedData.email, parsedData.phone);
        if (duplicate) {
          shouldUpdate = true;
          existingCandidateId = duplicate.id;
          logger.info("Found existing candidate", { existingCandidateId });
        }
      }

      // 5) Check for GHL duplicate
      let ghlContactIdToUpdate = null;
      if (holdItem.extraction_data?.ghl_duplicate_contact_id) {
        ghlContactIdToUpdate = holdItem.extraction_data.ghl_duplicate_contact_id;
        logger.info("GHL duplicate detected - will update existing contact", {
          ghlContactId: ghlContactIdToUpdate,
        });
      }

      // 6) Write/update candidate in Supabase
      let candidateId: string;

      if (shouldUpdate && existingCandidateId) {
        await updateCandidate(existingCandidateId, {
          ...parsedData,
          cv_file_path: holdItem.cv_file_path,
          cv_raw_text: holdItem.cv_raw_text,
          batch_id: holdItem.batch_id,
          status: "pending_ghl_sync",
        });
        candidateId = existingCandidateId;
        logger.info("✅ Updated existing candidate in Supabase", { candidateId });
      } else {
        candidateId = await writeCandidate({
          ...parsedData,
          client_id: holdItem.client_id || null,
          ghl_contact_id: null,
          cv_file_path: holdItem.cv_file_path,
          cv_raw_text: holdItem.cv_raw_text,
          batch_id: holdItem.batch_id,
          status: "pending_ghl_sync",
        });
        logger.info("✅ Created new candidate in Supabase", { candidateId });
      }

      // 7) Sync to GHL
      const ghlAccessToken = ENV.GHL_PRIVATE_KEY;
      let ghlContactId = null;

      try {
        // If we already have a GHL contact ID from duplicate detection, use it
        if (ghlContactIdToUpdate) {
          ghlContactId = ghlContactIdToUpdate;
          await updateGHLContact(ghlContactId, parsedData, ghlAccessToken);
          await trackGHLCall("update_contact", holdItem.batch_id);
          logger.info("✅ Updated existing GHL contact (from duplicate detection)", {
            ghlContactId,
          });
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
            await updateGHLContact(ghlContactId, parsedData, ghlAccessToken);
            await trackGHLCall("update_contact", holdItem.batch_id);
            logger.info("✅ Updated existing GHL contact", { ghlContactId });
          } else {
            ghlContactId = await createGHLContact(parsedData, ghlAccessToken);
            await trackGHLCall("create_contact", holdItem.batch_id);
            await updateGHLContact(ghlContactId, parsedData, ghlAccessToken);
            await trackGHLCall("update_contact", holdItem.batch_id);
            logger.info("✅ Created new GHL contact", { ghlContactId });
          }
        }

        // Upload CV file
        if (ghlContactId) {
          const uploadResult = await uploadCVToGHL(
            ghlContactId,
            holdItem.cv_file_path,
            holdItem.file_name,
            ghlAccessToken
          );
          await trackGHLCall("upload_file", holdItem.batch_id);

          if (!uploadResult.success) {
            logger.warn("⚠️ CV file upload failed", {
              ghlContactId,
              error: uploadResult.error,
            });
          }
        }

        await updateCandidateGHL(candidateId, ghlContactId, "complete");
        logger.info("✅ GHL sync successful", { candidateId, ghlContactId });
      } catch (ghlError: any) {
        logger.error("❌ GHL sync failed", {
          error: ghlError.message,
          stack: ghlError.stack,
          candidateId,
        });

        await updateCandidateGHL(candidateId, null, "ghl_sync_failed");
      }

      // 8) Mark hold queue item as complete
      await updateHoldQueueStatus(holdQueueId, "complete", candidateId);
      logger.info("✅ Hold queue item processed successfully", {
        holdQueueId,
        candidateId,
      });

      // 9) Log batch cost summary
      if (holdItem.batch_id) {
        await logBatchCostSummary(holdItem.batch_id);
      }

      return { success: true, candidateId, ghlContactId };
    } catch (error: any) {
      logger.error("❌ Hold queue processing failed", {
        holdQueueId,
        error: error.message,
        stack: error.stack,
      });

      // Mark as failed
      try {
        await updateHoldQueueStatus(holdQueueId, "failed", null);
      } catch (updateError) {
        logger.error("Failed to update hold queue status", {
          holdQueueId,
          error: updateError,
        });
      }

      throw error;
    }
  },
});

// ============================================
// DATABASE OPERATIONS
// ============================================

async function fetchHoldQueueItem(holdQueueId: string): Promise<any> {
  const response = await fetch(
    `${ENV.SUPABASE_URL}/rest/v1/hold_queue?id=eq.${holdQueueId}`,
    {
      headers: {
        Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
        apikey: ENV.SUPABASE_SERVICE_KEY,
      },
    }
  );

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to fetch hold_queue item: ${errorText}`);
  }

  const result = await response.json();
  return result[0] || null;
}

async function findExistingCandidate(
  email: string | null,
  phone: string | null
): Promise<any> {
  if (!email && !phone) return null;

  let url = `${ENV.SUPABASE_URL}/rest/v1/candidates?`;

  if (email && phone) {
    url += `or=(email.eq.${encodeURIComponent(email)},phone.eq.${encodeURIComponent(phone)})`;
  } else if (email) {
    url += `email=eq.${encodeURIComponent(email)}`;
  } else if (phone) {
    url += `phone=eq.${encodeURIComponent(phone)}`;
  }

  url += "&limit=1";

  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
      apikey: ENV.SUPABASE_SERVICE_KEY,
    },
  });

  if (!response.ok) return null;

  const result = await response.json();
  return result[0] || null;
}

async function writeCandidate(candidateData: any): Promise<string> {
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

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Failed to write candidate: ${errorText.slice(0, 800)}`);
  }

  const result = await response.json();
  return result[0]?.id;
}

async function updateCandidate(candidateId: string, updates: any): Promise<void> {
  await fetch(`${ENV.SUPABASE_URL}/rest/v1/candidates?id=eq.${candidateId}`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
      apikey: ENV.SUPABASE_SERVICE_KEY,
      "Content-Type": "application/json",
      Prefer: "return=minimal",
    },
    body: JSON.stringify(updates),
  });
}

async function updateCandidateGHL(
  candidateId: string,
  ghlContactId: string | null,
  status: string
): Promise<void> {
  await fetch(`${ENV.SUPABASE_URL}/rest/v1/candidates?id=eq.${candidateId}`, {
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
}

async function updateHoldQueueStatus(
  holdQueueId: string,
  status: string,
  candidateId?: string | null
): Promise<void> {
  const updates: any = {
    status,
    processed_at: new Date().toISOString(),
  };

  if (candidateId) {
    updates.existing_candidate_id = candidateId;
  }

  await fetch(`${ENV.SUPABASE_URL}/rest/v1/hold_queue?id=eq.${holdQueueId}`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${ENV.SUPABASE_SERVICE_KEY}`,
      apikey: ENV.SUPABASE_SERVICE_KEY,
      "Content-Type": "application/json",
      Prefer: "return=minimal",
    },
    body: JSON.stringify(updates),
  });
}

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

      if (!retryable || attempt === maxAttempts) {
        throw new Error(`Gemini request failed (status ${status}): ${bodyText.slice(0, 500)}`);
      }

      const waitMs = getRetryDelay(attempt, GEMINI_CONFIG);
      await delay(waitMs);
    } catch (error: any) {
      if (attempt === maxAttempts) {
        throw error;
      }
      const waitMs = getRetryDelay(attempt, GEMINI_CONFIG);
      await delay(waitMs);
    }
  }

  throw new Error("Gemini request failed after retries");
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
    logger.error("Failed to parse Gemini output", {
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
  accessToken: string
): Promise<void> {
  const customFieldsData = buildCompleteGHLCustomFields(data);

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

function guessUploadMimeType(filename: string): string {
  const ext = filename.split(".").pop()?.toLowerCase();
  if (ext === "pdf") return "application/pdf";
  if (ext === "docx")
    return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
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

    const uploadResponse = await fetch(
      `${GHL_CONFIG.BASE_URL}/contacts/${contactId}/files`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${accessToken}`,
          Version: GHL_CONFIG.API_VERSION,
        },
        body: formData as any,
      }
    );

    if (!uploadResponse.ok) {
      const errorText = await uploadResponse.text();
      throw new Error(
        `GHL upload failed: ${uploadResponse.status} - ${errorText.slice(0, 800)}`
      );
    }

    const uploadResult = await uploadResponse.json();

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
