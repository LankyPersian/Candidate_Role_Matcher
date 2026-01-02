import { task, logger } from "@trigger.dev/sdk";

const SUPABASE_URL = "https://nxlzdqskcqbikzpxhjam.supabase.co";
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;
const GHL_PRIVATE_KEY = process.env.GHL_PRIVATE_INTEGRATION_KEY!;
const GHL_LOCATION_ID = process.env.GHL_LOCATION_ID!;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY!;

function delay(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

export const processHoldQueueItem = task({
  id: "process-hold-queue-item",
  run: async (payload: { holdQueueId: string }) => {
    const { holdQueueId } = payload;
    
    logger.info("Processing hold_queue item", { holdQueueId });

    const holdItem = await fetchHoldQueueItem(holdQueueId);
    
    if (!holdItem) {
      logger.error("Hold queue item not found", { holdQueueId });
      return { success: false, error: "Item not found" };
    }

    if (holdItem.status !== "ready_for_processing") {
      logger.warn("Hold queue item not ready", { holdQueueId, status: holdItem.status });
      return { success: false, error: "Not ready for processing" };
    }

    logger.info("Hold queue item retrieved", {
      holdQueueId,
      fileName: holdItem.file_name,
    });

    let parsedData: ParsedCV;
    
    if (holdItem.cv_raw_text && holdItem.cv_raw_text.trim().length > 50) {
      logger.info("Running full parse on raw text");
      parsedData = await fullParse(holdItem.cv_raw_text);
    } else {
      logger.warn("No raw text available");
      parsedData = getEmptyParsedCV();
    }

    const manualInfo = holdItem.manual_contact_info || {};
    parsedData.full_name = manualInfo.full_name || parsedData.full_name || holdItem.extracted_name;
    parsedData.email = manualInfo.email || parsedData.email;
    parsedData.phone = manualInfo.phone || parsedData.phone;

    logger.info("Final candidate data", {
      full_name: parsedData.full_name,
      email: parsedData.email,
      phone: parsedData.phone,
    });

    let shouldUpdate = false;
    let existingCandidateId = null;

    if (holdItem.duplicate_candidate_id) {
      shouldUpdate = true;
      existingCandidateId = holdItem.duplicate_candidate_id;
      logger.info("Marked to update existing candidate", { existingCandidateId });
    } else if (!holdItem.ignore_duplicate) {
      const duplicate = await findExistingCandidate(parsedData.email, parsedData.phone);
      if (duplicate) {
        shouldUpdate = true;
        existingCandidateId = duplicate.id;
        logger.info("Found existing candidate", { existingCandidateId });
      }
    }

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
      logger.info("Updated existing candidate in Supabase", { candidateId });
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
      logger.info("Created new candidate in Supabase", { candidateId });
    }

    const ghlAccessToken = GHL_PRIVATE_KEY;
    let ghlContactId = null;

    try {
      const existingContactId = await findExistingGHLContact(
        parsedData.email,
        parsedData.phone,
        ghlAccessToken
      );

      if (existingContactId) {
        ghlContactId = existingContactId;
        await updateGHLContact(ghlContactId, parsedData, ghlAccessToken);
        logger.info("Updated existing GHL contact", { ghlContactId });
      } else {
        ghlContactId = await createGHLContact(parsedData, ghlAccessToken);
        await updateGHLContact(ghlContactId, parsedData, ghlAccessToken);
        logger.info("Created new GHL contact", { ghlContactId });
      }

      await updateCandidateGHL(candidateId, ghlContactId, "complete");
      logger.info("GHL sync successful", { candidateId, ghlContactId });
    } catch (ghlError: any) {
      logger.error("GHL sync failed", {
        error: ghlError.message,
        candidateId,
      });
    }

    await updateHoldQueueStatus(holdQueueId, "complete", candidateId);
    logger.info("Hold queue item processed successfully", { holdQueueId, candidateId });

    return { success: true, candidateId, ghlContactId };
  },
});

async function fetchHoldQueueItem(holdQueueId: string): Promise<any> {
  const response = await fetch(
    `${SUPABASE_URL}/rest/v1/hold_queue?id=eq.${holdQueueId}`,
    {
      headers: {
        Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
        apikey: SUPABASE_SERVICE_KEY,
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

async function findExistingCandidate(email: string | null, phone: string | null): Promise<any> {
  if (!email && !phone) return null;

  let url = `${SUPABASE_URL}/rest/v1/candidates?`;
  
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
      Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
      apikey: SUPABASE_SERVICE_KEY,
    },
  });

  if (!response.ok) return null;

  const result = await response.json();
  return result[0] || null;
}

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

    if (!retryable || attempt === maxAttempts) {
      throw new Error(`Gemini request failed (status ${status}): ${bodyText.slice(0, 500)}`);
    }

    const baseMs = 1200;
    const backoffMs = Math.round(baseMs * Math.pow(2, attempt - 1));
    const jitterMs = Math.floor(Math.random() * 400);
    const waitMs = Math.min(20000, backoffMs + jitterMs);

    await delay(waitMs);
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
    logger.error("Failed to parse Gemini output", { preview: cleaned.slice(0, 600) });
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

async function findExistingGHLContact(
  email: string | null,
  phone: string | null,
  accessToken: string
): Promise<string | null> {
  if (email && isValidEmail(email)) {
    const emailUrl = `https://services.leadconnectorhq.com/contacts/?locationId=${GHL_LOCATION_ID}&query=${encodeURIComponent(email)}`;
    
    const emailResponse = await fetch(emailUrl, {
      headers: {
        Authorization: `Bearer ${accessToken}`,
        Version: "2021-07-28",
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
      const phoneUrl = `https://services.leadconnectorhq.com/contacts/?locationId=${GHL_LOCATION_ID}&query=${encodeURIComponent(normalized)}`;
      
      const phoneResponse = await fetch(phoneUrl, {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          Version: "2021-07-28",
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

        logger.info("GHL contact created successfully", { contactId: id });
        return id;
      }

      const status = response.status;
      const retryable = status === 429 || status === 503 || status === 500;

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
    throw new Error(`GHL contact update failed: ${txt.slice(0, 800)}`);
  }
}

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

async function updateCandidate(candidateId: string, updates: any): Promise<void> {
  await fetch(`${SUPABASE_URL}/rest/v1/candidates?id=eq.${candidateId}`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
      apikey: SUPABASE_SERVICE_KEY,
      "Content-Type": "application/json",
      Prefer: "return=minimal",
    },
    body: JSON.stringify(updates),
  });
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

async function updateHoldQueueStatus(
  holdQueueId: string,
  status: string,
  candidateId?: string
): Promise<void> {
  const updates: any = {
    status,
    processed_at: new Date().toISOString(),
  };

  if (candidateId) {
    updates.existing_candidate_id = candidateId;
  }

  await fetch(`${SUPABASE_URL}/rest/v1/hold_queue?id=eq.${holdQueueId}`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
      apikey: SUPABASE_SERVICE_KEY,
      "Content-Type": "application/json",
      Prefer: "return=minimal",
    },
    body: JSON.stringify(updates),
  });
}