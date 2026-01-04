// processCVBatch.ts
import { task, logger } from "@trigger.dev/sdk";
import { Buffer } from "buffer";
import { buildGHLCustomFields } from "./ghl-transformers";

const SUPABASE_URL = "https://nxlzdqskcqbikzpxhjam.supabase.co";
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY!;
const GHL_PRIVATE_KEY = process.env.GHL_PRIVATE_INTEGRATION_KEY!;
const GHL_LOCATION_ID = process.env.GHL_LOCATION_ID!;

// Helper: small delay to avoid rate limits / backoff
function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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

type HoldQueueDuplicateDetails = {
  full_name: string;
  email: string | null;
  phone: string | null;
  updated_at: string;
};

// ============================================
// MAIN TASK
// ============================================
export const processCVBatch = task({
  id: "process-cv-batch",
  maxDuration: 600,
  run: async (payload: CVBatchPayload) => {
    const { batchId, clientId } = payload;

    logger.info("Starting CV batch processing", { batchId, clientId });

    // Use the access token directly (it's already in the env var)
    const ghlAccessToken = GHL_PRIVATE_KEY;
    logger.info("Using GHL access token directly", {
      tokenPrefix: ghlAccessToken.substring(0, 15) + "...",
    });

    // 1) List all files
    const files = await listBatchFiles(batchId, clientId);
    logger.info(`Found ${files.length} files to process`, { batchId });

    // 2) Mark batch processing
    await updateBatchStatus(batchId, "processing");

    let processed = 0; // candidates written to Supabase
    let failed = 0; // unrecoverable file-level failures
    let heldForReview = 0; // inserted into hold_queue

    // 3) Process each file (sequential to reduce rate limiting)
    for (const file of files) {
      try {
        logger.info("Processing file", { name: file.name, path: file.path });

        const fileBuffer = await downloadFile(file.path);
        const rawText = await extractText(fileBuffer, file.name);

        logger.info("Extracted text stats", {
          file: file.name,
          length: rawText?.length ?? 0,
          preview: (rawText || "").slice(0, 200),
        });

        if (!rawText || rawText.trim().length < 50) {
          logger.warn("File has insufficient text", { file: file.name });
          failed++;
          continue;
        }

        // Quick parse (regex first, Gemini only if needed)
        const contactInfo = await quickParse(rawText);

        if (!contactInfo.email && !contactInfo.phone) {
          logger.warn("Missing contact info -> inserting to hold_queue NOW", {
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
            },
          });

          heldForReview++;
          continue;
        }

        // Full parse
        const parsedData = await fullParse(rawText);

        // Prefer fullParse but fall back to quickParse for identity basics
        parsedData.full_name = parsedData.full_name ?? contactInfo.full_name;
        parsedData.email = parsedData.email ?? contactInfo.email;
        parsedData.phone = parsedData.phone ?? contactInfo.phone;

        // 🔥 CHECK GHL FOR DUPLICATES FIRST (before writing to Supabase)
        let existingGHLContactId: string | null = null;
        try {
          existingGHLContactId = await findExistingGHLContact(
            parsedData.email,
            parsedData.phone,
            ghlAccessToken
          );
        } catch (err: any) {
          logger.warn("GHL duplicate check failed, proceeding without duplicate detection", {
            file: file.name,
            error: err?.message ?? String(err),
          });
        }

        // Fetch full GHL contact details if duplicate found
        let ghlContactDetails: HoldQueueDuplicateDetails | null = null;
        if (existingGHLContactId) {
          ghlContactDetails = await fetchGHLContactDetails(existingGHLContactId, ghlAccessToken, {
            fallbackEmail: parsedData.email,
            fallbackPhone: parsedData.phone,
          });
        }

        // If GHL duplicate found, add to hold_queue for user review
        if (existingGHLContactId) {
          logger.warn("GHL duplicate detected -> adding to hold_queue for review", {
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
            },
          });

          heldForReview++;
          continue;
        }

        // No GHL duplicate - proceed with normal processing
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

        logger.info("Candidate saved to Supabase", { file: file.name, candidateId });

        // Create new GHL contact and update custom fields + upload CV
        let ghlContactId: string | null = null;

        try {
          ghlContactId = await createGHLContact(parsedData, ghlAccessToken);

          await updateGHLContact(ghlContactId, parsedData, ghlAccessToken);

          const uploadResult = await uploadCVToGHL(
            ghlContactId,
            file.path,
            file.name,
            ghlAccessToken
          );

          if (!uploadResult.success) {
            logger.warn("CV file upload failed", {
              file: file.name,
              contactId: ghlContactId,
              error: uploadResult.error,
            });
          }

          await updateCandidateGHL(candidateId, ghlContactId, "complete");

          logger.info("GHL sync successful", {
            file: file.name,
            ghlContactId,
            cvUploaded: uploadResult.success,
          });
        } catch (ghlError: any) {
          logger.error("GHL sync failed, candidate saved to Supabase only", {
            file: file.name,
            candidateId,
            error: ghlError?.message ?? String(ghlError),
          });

          // Ensure candidate reflects failure
          await updateCandidateGHL(candidateId, null, "ghl_sync_failed");
        }

        processed++;
        await updateBatchProgress(batchId, processed, files.length);
      } catch (error: any) {
        logger.error("Failed to process file", {
          file: file?.name,
          error: error?.message ?? String(error),
        });
        failed++;
      }
    }

    // 4) Complete
    const finalStatus = heldForReview > 0 ? "awaiting_input" : "complete";
    await updateBatchStatus(batchId, finalStatus, processed);

    logger.info("Batch processing complete", {
      processed,
      failed,
      held: heldForReview,
      batchId,
    });

    return { processed, failed, held: heldForReview, batchId };
  },
});

// ============================================
// HELPERS: SUPABASE STORAGE
// ============================================

function encodeStoragePath(path: string) {
  return path.split("/").map(encodeURIComponent).join("/");
}

async function listBatchFiles(batchId: string, clientId: string) {
  function safeJsonParse(text: string): any {
    try {
      return JSON.parse(text);
    } catch {
      return null;
    }
  }

  function decodeJwtRole(jwt: string | undefined): string | null {
    if (!jwt || typeof jwt !== "string") return null;
    const parts = jwt.split(".");
    if (parts.length < 2) return null;

    try {
      const payload = parts[1]
        .replace(/-/g, "+")
        .replace(/_/g, "/")
        .padEnd(Math.ceil(parts[1].length / 4) * 4, "=");

      const json = Buffer.from(payload, "base64").toString("utf8");
      const obj = JSON.parse(json);
      return obj?.role ?? null;
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

    const res = await fetch(`${SUPABASE_URL}/storage/v1/object/list/cv-uploads`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
        apikey: SUPABASE_SERVICE_KEY,
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
      status: res.status,
      text,
      parsed,
      items: normalizeListResponse(parsed),
    };
  }

  const role = decodeJwtRole(SUPABASE_SERVICE_KEY);
  logger.info("Storage listBatchFiles starting", {
    batchId,
    clientId,
    supabaseKeyRole: role,
    supabaseKeyLen: (SUPABASE_SERVICE_KEY || "").length,
  });

  if (role !== "service_role") {
    throw new Error(
      `SUPABASE_SERVICE_KEY is not service_role (got: ${role ?? "unknown"}). ` +
        `Use the Service Role key in Trigger.dev env vars (server-only).`
    );
  }

  let effectiveClientId = (clientId || "").trim();

  const root = await callList({ prefix: "" });
  const rootNames = root.items.map(toName).filter(Boolean) as string[];

  logger.info("Storage root snapshot", {
    rootCount: rootNames.length,
    sampleRoot: rootNames.slice(0, 30),
  });

  if (!effectiveClientId) {
    if (rootNames.length === 1) {
      effectiveClientId = rootNames[0];
      logger.warn("clientId missing; auto-using sole root folder", { effectiveClientId });
    }
  }

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

    logger.info("Storage list attempt", {
      prefix: prefixToTry,
      normalizedCount: r.items.length,
      sampleNames: r.items.map(toName).filter(Boolean).slice(0, 10),
      rawPreview: (r.text || "").slice(0, 250),
    });

    if (r.items.length > 0) {
      chosenPrefix = prefixToTry;
      chosenItems = r.items;
      break;
    }
  }

  const actualFiles = chosenItems.filter(isLikelyFileItem);

  logger.info("Storage list final selection", {
    prefixUsed: chosenPrefix,
    returned: chosenItems.length,
    actualFiles: actualFiles.length,
    sampleActual: actualFiles.map(toName).filter(Boolean).slice(0, 10),
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

  const response = await fetch(`${SUPABASE_URL}/storage/v1/object/cv-uploads/${encodedPath}`, {
    headers: {
      Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
      apikey: SUPABASE_SERVICE_KEY,
    },
  });

  if (!response.ok) {
    const err = await response.text();
    throw new Error(`Failed to download file: ${response.status} ${err.slice(0, 800)}`);
  }

  return response.arrayBuffer();
}

// ============================================
// HELPERS: GEMINI
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
  const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${GEMINI_API_KEY}`;
  const maxAttempts = 6;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
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
    const retryable = status === 429 || status === 503 || status === 500;

    logger.error("Gemini request failed", {
      attempt,
      status,
      bodyPreview: bodyText.slice(0, 800),
    });

    if (!retryable || attempt === maxAttempts) {
      throw new Error(`Gemini request failed (status ${status}): ${bodyText.slice(0, 500)}`);
    }

    const retryAfterHeader = response.headers.get("retry-after");
    const retryAfterSeconds = retryAfterHeader ? Number(retryAfterHeader) : NaN;

    const baseMs = 1200;
    const backoffMs = Math.round(baseMs * Math.pow(2, attempt - 1));
    const jitterMs = Math.floor(Math.random() * 400);

    const waitMs = Number.isFinite(retryAfterSeconds)
      ? Math.max(1000, retryAfterSeconds * 1000)
      : Math.min(20000, backoffMs + jitterMs);

    logger.warn("Retrying Gemini request after backoff", { attempt, waitMs, status });
    await delay(waitMs);
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
      { text: "Extract all text content from this document. Return only the raw text, no formatting or commentary." },
    ],
    temperature: 0,
  });

  await delay(1500);
  return result;
}

// ============================================
// PARSING
// ============================================

function looksLikeYearRange(s: string): boolean {
  return /\b(19|20)\d{2}\s*[-–]\s*(19|20)\d{2}\b/.test(s);
}

function selectBestPhoneCandidate(matches: string[]): string | null {
  const cleaned = matches
    .map((m) => m.trim())
    .filter((m) => m.length >= 10)
    .filter((m) => !looksLikeYearRange(m))
    .map((m) => ({ raw: m, digits: m.replace(/\D/g, "") }))
    .filter((x) => x.digits.length >= 10 && x.digits.length <= 15);

  if (cleaned.length === 0) return null;

  // Prefer candidates that include "+" or "(" or separators (more phone-like)
  cleaned.sort((a, b) => {
    const score = (x: { raw: string; digits: string }) => {
      let s = 0;
      if (x.raw.includes("+")) s += 3;
      if (x.raw.includes("(") || x.raw.includes(")")) s += 2;
      if (/[\s.-]/.test(x.raw)) s += 1;
      // prefer UK-length or international-ish
      if (x.digits.length === 11 || x.digits.length === 12) s += 1;
      return s;
    };
    return score(b) - score(a);
  });

  return cleaned[0].raw.replace(/\s+/g, " ").trim();
}

async function quickParse(rawText: string): Promise<{ full_name: string | null; email: string | null; phone: string | null }> {
  const emailMatch = rawText.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);

  // Find phone-ish candidates; then filter out year ranges and too-short digit strings
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

  await delay(1500);

  const cleaned = stripJsonFences(output);

  try {
    const parsed = JSON.parse(cleaned);
    return {
      full_name: parsed.full_name ?? null,
      email: parsed.email ?? null,
      phone: parsed.phone ?? null,
    };
  } catch {
    logger.warn("quickParse: Gemini returned non-JSON content", { preview: cleaned.slice(0, 300) });
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
${rawText.substring(0, 8000)}`,
      },
    ],
    responseMimeType: "application/json",
    temperature: 0,
  });

  await delay(1500);

  const cleaned = stripJsonFences(output);

  try {
    return JSON.parse(cleaned);
  } catch {
    logger.error("Failed to parse Gemini output (fullParse)", { preview: cleaned.slice(0, 600) });
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
// HELPERS: GHL
// ============================================

function isValidEmail(email: string) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email.trim());
}

function normalizePhone(phone: string) {
  if (!phone) return null;

  let cleaned = phone.trim().replace(/[\s\-\(\)\.]/g, "");

  cleaned = cleaned.replace(/^\+/, "");
  cleaned = cleaned.replace(/\(0\)/, "");

  if (cleaned.startsWith("0") && cleaned.length === 11) {
    cleaned = "44" + cleaned.substring(1);
  }

  if (cleaned.startsWith("44") && cleaned.length === 12) {
    return cleaned;
  }

  return cleaned.length >= 10 ? cleaned : null;
}

async function findExistingGHLContact(
  email: string | null,
  phone: string | null,
  accessToken: string
): Promise<string | null> {
  // Search by email first
  if (email && isValidEmail(email)) {
    const emailUrl = `https://services.leadconnectorhq.com/contacts/?locationId=${GHL_LOCATION_ID}&query=${encodeURIComponent(
      email
    )}`;

    const emailResponse = await fetch(emailUrl, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        Version: "2021-07-28",
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

  // Search by phone
  if (phone) {
    const normalized = normalizePhone(phone);
    if (normalized) {
      const phoneUrl = `https://services.leadconnectorhq.com/contacts/?locationId=${GHL_LOCATION_ID}&query=${encodeURIComponent(
        normalized
      )}`;

      const phoneResponse = await fetch(phoneUrl, {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          Version: "2021-07-28",
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

  logger.info("No existing GHL contact found", { email, phone });
  return null;
}

async function fetchGHLContactDetails(
  contactId: string,
  accessToken: string,
  opts: { fallbackEmail: string | null; fallbackPhone: string | null }
): Promise<HoldQueueDuplicateDetails | null> {
  try {
    const response = await fetch(`https://services.leadconnectorhq.com/contacts/${contactId}`, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        Version: "2021-07-28",
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
  const nameParts = (data.full_name || "Unknown").trim().split(/\s+/);
  const firstName = nameParts[0] || "Unknown";
  const lastName = nameParts.slice(1).join(" ") || "";

  const email = data.email && isValidEmail(data.email) ? data.email.trim() : undefined;
  const phone = data.phone ? normalizePhone(data.phone) ?? undefined : undefined;

  const basePayload: any = {
    firstName,
    lastName,
    locationId: GHL_LOCATION_ID,
    tags: ["cv-imported"],
  };

  const variants: any[] = [
    { ...basePayload, ...(email ? { email } : {}), ...(phone ? { phone } : {}) },
    { ...basePayload, ...(email ? { email } : {}) },
    { ...basePayload, ...(phone ? { phone } : {}) },
    { ...basePayload },
  ];

  const url = `https://services.leadconnectorhq.com/contacts/`;
  const maxAttempts = 4;

  for (const payload of variants) {
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      const response = await fetch(url, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${accessToken}`,
          "Content-Type": "application/json",
          Version: "2021-07-28",
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
      const retryable = status === 429 || status === 503 || status === 500;

      logger.error("GHL contact creation failed", {
        attempt,
        status,
        body: bodyText.slice(0, 2000),
        payloadSent: payload,
      });

      if (!retryable) break;
      if (attempt === maxAttempts) break;

      const baseMs = 800;
      const backoffMs = Math.round(baseMs * Math.pow(2, attempt - 1));
      const jitterMs = Math.floor(Math.random() * 250);
      const waitMs = Math.min(8000, backoffMs + jitterMs);

      await delay(waitMs);
    }
  }

  throw new Error("GHL contact creation failed after trying payload variants");
}

// ✅ FIXED: Complete updateGHLContact function (no dangling block)
async function updateGHLContact(
  contactId: string,
  data: ParsedCV,
  accessToken: string
): Promise<void> {
  // Build all custom fields with formatted text using the transformer
  const customFieldsData = buildGHLCustomFields(data);

  // Convert to GHL's expected format: array of {key, value} objects
  const customFields = Object.entries(customFieldsData).map(([key, value]) => ({
    key,
    value: value || "", // GHL requires empty string, not null
  }));

  const response = await fetch(`https://services.leadconnectorhq.com/contacts/${contactId}`, {
    method: "PUT",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json",
      Version: "2021-07-28",
    },
    body: JSON.stringify({ customFields }),
  });

  if (!response.ok) {
    const txt = await response.text();
    logger.error("GHL contact update failed", {
      contactId,
      status: response.status,
      body: txt.slice(0, 1200),
    });
    throw new Error(`GHL contact update failed: ${txt.slice(0, 800)}`);
  }

  logger.info("GHL contact updated with formatted fields", {
    contactId,
    fieldsUpdated: customFields.length,
  });
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
    // Download file from Supabase Storage
    const encodedPath = cvFilePath.split("/").map(encodeURIComponent).join("/");
    const downloadUrl = `${SUPABASE_URL}/storage/v1/object/cv-uploads/${encodedPath}`;

    const downloadResponse = await fetch(downloadUrl, {
      headers: {
        Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
        apikey: SUPABASE_SERVICE_KEY,
      },
    });

    if (!downloadResponse.ok) {
      const t = await downloadResponse.text();
      throw new Error(`Failed to download CV: ${downloadResponse.status} - ${t.slice(0, 300)}`);
    }

    const fileBuffer = await downloadResponse.arrayBuffer();

    // Avoid TS/DOM lib issues by using any constructors
    const BlobCtor = (globalThis as any).Blob;
    const FormDataCtor = (globalThis as any).FormData;

    if (!BlobCtor || !FormDataCtor) {
      throw new Error("Blob/FormData not available in this runtime (Node/undici).");
    }

    const mimeType = guessUploadMimeType(originalFilename);
    const blob = new BlobCtor([fileBuffer], { type: mimeType });
    const formData = new FormDataCtor();
    formData.append("file", blob, originalFilename);

    const uploadResponse = await fetch(
      `https://services.leadconnectorhq.com/contacts/${contactId}/files`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${accessToken}`,
          Version: "2021-07-28",
          // Do not set Content-Type manually for multipart
        },
        body: formData as any,
      }
    );

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
// HELPERS: SUPABASE DB
// ============================================

async function writeCandidate(candidateData: any): Promise<string> {
  const response = await fetch(`${SUPABASE_URL}/rest/v1/candidates`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
      apikey: SUPABASE_SERVICE_KEY,
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
  const id = result?.[0]?.id;
  if (!id) throw new Error("Candidate insert succeeded but no id returned");
  return id;
}

// ✅ NOTE: ghlContactId is nullable to support ghl_sync_failed path
async function updateCandidateGHL(
  candidateId: string,
  ghlContactId: string | null,
  status: string
): Promise<void> {
  const response = await fetch(`${SUPABASE_URL}/rest/v1/candidates?id=eq.${candidateId}`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
      apikey: SUPABASE_SERVICE_KEY,
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
    logger.error("Failed to update candidate GHL/status", {
      candidateId,
      ghlContactId,
      status,
      respStatus: response.status,
      body: t.slice(0, 600),
    });
    throw new Error(`Failed to update candidate GHL/status: ${t.slice(0, 800)}`);
  }
}

async function updateBatchStatus(batchId: string, status: string, processedCount?: number): Promise<void> {
  const updateData: any = { status };
  if (processedCount !== undefined) updateData.processed_count = processedCount;
  if (status === "complete" || status === "awaiting_input") updateData.completed_at = new Date().toISOString();

  const response = await fetch(`${SUPABASE_URL}/rest/v1/processing_batches?id=eq.${batchId}`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
      apikey: SUPABASE_SERVICE_KEY,
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

async function updateBatchProgress(batchId: string, processed: number, _total: number): Promise<void> {
  const response = await fetch(`${SUPABASE_URL}/rest/v1/processing_batches?id=eq.${batchId}`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
      apikey: SUPABASE_SERVICE_KEY,
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

  const response = await fetch(`${SUPABASE_URL}/rest/v1/hold_queue`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
      apikey: SUPABASE_SERVICE_KEY,
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
