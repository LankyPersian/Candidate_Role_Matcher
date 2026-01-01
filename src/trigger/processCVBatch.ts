import { task, logger } from "@trigger.dev/sdk";

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
      tokenPrefix: ghlAccessToken.substring(0, 15) + "..."
    });

    // 1) List all files
    const files = await listBatchFiles(batchId, clientId);
    logger.info(`Found ${files.length} files to process`, { batchId });

    // 2) Mark batch processing
    await updateBatchStatus(batchId, "processing");

    let processed = 0;
    let failed = 0;
    const holdQueue: any[] = [];

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
          logger.warn("Missing contact info -> sending to hold queue", {
            file: file.name,
            extracted: contactInfo,
          });
          holdQueue.push({
            file_name: file.name,
            file_path: file.path,
            extracted_name: contactInfo.full_name,
            raw_text: rawText,
          });
          continue;
        }

        // Full parse
        const parsedData = await fullParse(rawText);

        // SAVE TO SUPABASE FIRST (ensures data is never lost)
        const candidateId = await writeCandidate({
          ...parsedData,
          client_id: clientId || null,
          ghl_contact_id: null, // Will be filled after GHL sync
          cv_file_path: file.path,
          cv_raw_text: rawText,
          batch_id: batchId,
          status: "pending_ghl_sync",
        });

        logger.info("Candidate saved to Supabase", { file: file.name, candidateId });

        // TRY GHL sync (non-blocking - won't stop processing if it fails)
        let ghlContactId = null;
        try {
          ghlContactId = await createGHLContact(parsedData, ghlAccessToken);
          await updateGHLContact(ghlContactId, parsedData, ghlAccessToken);
          
          // Update candidate with GHL ID and status
          await updateCandidateGHL(candidateId, ghlContactId, "complete");
          
          logger.info("GHL sync successful", { file: file.name, ghlContactId });
        } catch (ghlError: any) {
          logger.error("GHL sync failed, but candidate saved to Supabase", {
            file: file.name,
            error: ghlError.message,
            candidateId,
          });
          // Don't throw - continue processing other files
        }

        processed++;
        await updateBatchProgress(batchId, processed, files.length);
      } catch (error: any) {
        logger.error("Failed to process file", {
          file: file?.name,
          error: error?.message || error,
        });
        failed++;
      }
    }

    // 4) Hold queue
    if (holdQueue.length > 0) {
      await addToHoldQueue(batchId, clientId, holdQueue);
    }

    // 5) Complete
    const finalStatus = holdQueue.length > 0 ? "awaiting_input" : "complete";
    await updateBatchStatus(batchId, finalStatus, processed);

    logger.info("Batch processing complete", {
      processed,
      failed,
      held: holdQueue.length,
      batchId,
    });

    return { processed, failed, held: holdQueue.length, batchId };
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

async function extractText(buffer: ArrayBuffer, fileName: string): Promise<string> {
  const extension = fileName.split(".").pop()?.toLowerCase();

  if (extension === "txt") return new TextDecoder().decode(buffer);

  const base64 = Buffer.from(buffer).toString("base64");

  if (extension === "pdf") {
    logger.info("Gemini extractText called", {
      fileName,
      mimeType: "application/pdf",
      bytes: buffer.byteLength,
    });

    const result = await geminiGenerateText({
      parts: [
        { inlineData: { mimeType: "application/pdf", data: base64 } },
        { text: "Extract all text content from this document. Return only the raw text, no formatting or commentary." },
      ],
      temperature: 0,
    });

    await delay(1500);
    return result;
  }

  if (extension === "docx" || extension === "doc") {
    logger.info("Gemini extractText called", {
      fileName,
      mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
      bytes: buffer.byteLength,
    });

    const result = await geminiGenerateText({
      parts: [
        {
          inlineData: {
            mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            data: base64,
          },
        },
        { text: "Extract all text content from this document. Return only the raw text, no formatting or commentary." },
      ],
      temperature: 0,
    });

    await delay(1500);
    return result;
  }

  throw new Error(`Unsupported file type: ${extension}`);
}

async function quickParse(rawText: string): Promise<{ full_name: string | null; email: string | null; phone: string | null }> {
  const emailMatch = rawText.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  const phoneMatch = rawText.match(/(\+?\d[\d\s().-]{7,}\d)/);

  const regexResult = {
    full_name: null,
    email: emailMatch ? emailMatch[0] : null,
    phone: phoneMatch ? phoneMatch[0].replace(/\s+/g, " ").trim() : null,
  };

  logger.info("quickParse regex results", regexResult);

  if (regexResult.email || regexResult.phone) {
    await delay(1500);
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
  const cleaned = phone.trim().replace(/[^\d+]/g, "");
  if (!cleaned) return null;
  if (cleaned.startsWith("+") && cleaned.length >= 8) return cleaned;

  const digits = cleaned.replace(/[^\d]/g, "");
  return digits.length >= 8 ? digits : null;
}

async function createGHLContact(
  data: ParsedCV,
  accessToken: string
): Promise<string> {
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

        logger.info("GHL contact created successfully", { contactId: id, name: `${firstName} ${lastName}` });
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

async function updateGHLContact(
  contactId: string,
  data: ParsedCV,
  accessToken: string
): Promise<void> {
  const response = await fetch(`https://services.leadconnectorhq.com/contacts/${contactId}`, {
    method: "PUT",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json",
      Version: "2021-07-28",
    },
    body: JSON.stringify({
      customFields: [
        { key: "cv_summary", value: data.cv_summary || "" },
        { key: "current_job_title", value: data.work_history?.[0]?.job_title || "" },
        { key: "candidate_salary_expectation", value: data.salary_expectation || "" },
        { key: "current_notice_period", value: data.notice_period || "" },
        { key: "candidate_skills_summery", value: data.skills?.join(", ") || "" },
      ],
    }),
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
  return result[0]?.id;
}

async function updateCandidateGHL(candidateId: string, ghlContactId: string, status: string): Promise<void> {
  await fetch(`${SUPABASE_URL}/rest/v1/candidates?id=eq.${candidateId}`, {
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
}

async function updateBatchStatus(batchId: string, status: string, processedCount?: number): Promise<void> {
  const updateData: any = { status };
  if (processedCount !== undefined) updateData.processed_count = processedCount;
  if (status === "complete" || status === "awaiting_input") updateData.completed_at = new Date().toISOString();

  await fetch(`${SUPABASE_URL}/rest/v1/processing_batches?id=eq.${batchId}`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
      apikey: SUPABASE_SERVICE_KEY,
      "Content-Type": "application/json",
      Prefer: "return=minimal",
    },
    body: JSON.stringify(updateData),
  });
}

async function updateBatchProgress(batchId: string, processed: number, _total: number): Promise<void> {
  await fetch(`${SUPABASE_URL}/rest/v1/processing_batches?id=eq.${batchId}`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
      apikey: SUPABASE_SERVICE_KEY,
      "Content-Type": "application/json",
      Prefer: "return=minimal",
    },
    body: JSON.stringify({ processed_count: processed }),
  });
}

async function addToHoldQueue(batchId: string, clientId: string, items: any[]): Promise<void> {
  const records = items.map((item) => ({
    batch_id: batchId,
    client_id: clientId,
    extracted_name: item.extracted_name,
    cv_file_path: item.file_path,
    cv_raw_text: item.raw_text,
    status: "pending",
  }));

  await fetch(`${SUPABASE_URL}/rest/v1/hold_queue`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
      apikey: SUPABASE_SERVICE_KEY,
      "Content-Type": "application/json",
      Prefer: "return=minimal",
    },
    body: JSON.stringify(records),
  });
}