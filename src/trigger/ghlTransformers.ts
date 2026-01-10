// ============================================
// GHL DATA TRANSFORMATION FUNCTIONS (Professional)
// ============================================
// Converts Supabase JSON → Professional formatted text for GHL
// No emojis, concise formatting, complete 51-field mapping

import { GHL_FIELD_MAPPING } from "./config";

// ============================================
// INTERFACES
// ============================================

interface WorkHistoryItem {
  job_title?: string;
  company_name?: string;
  company_location?: string;
  start_date?: string;
  end_date?: string;
  employment_type?: string;
  duties_responsibilities?: string;
  achievements?: string;
  reason_for_leaving?: string;
}

interface EducationItem {
  qualification_name?: string;
  institution_name?: string;
  start_date?: string;
  end_date?: string;
  grade_classification?: string;
  honours_awards?: string;
  dissertation_thesis?: string;
  extracurricular?: string;
}

interface CertificationItem {
  name: string;
  issuing_organisation?: string;
  date_obtained?: string;
  expiry_date?: string;
  certification_id?: string;
}

interface LanguageItem {
  language: string;
  proficiency?: string;
  reading?: string;
  writing?: string;
  speaking?: string;
  certifications?: string;
}

interface TrainingCourseItem {
  course_name?: string;
  provider?: string;
  date_completed?: string;
  duration?: string;
  format?: string;
  accreditation?: string;
}

interface ProfessionalMembershipItem {
  organisation_name?: string;
  membership_type?: string;
  member_since?: string;
  member_number?: string;
}

// ============================================
// UTILITY FUNCTIONS
// ============================================

/**
 * Split full name into first and last name
 */
export function splitName(fullName: string | null): { firstName: string; lastName: string } {
  if (!fullName) return { firstName: "", lastName: "" };

  const parts = fullName.trim().split(" ");
  if (parts.length === 1) {
    return { firstName: parts[0], lastName: "" };
  }

  const lastName = parts.pop()!;
  const firstName = parts.join(" ");

  return { firstName, lastName };
}

/**
 * Calculate tenure category from work start date
 */
export function calculateTenure(startDate: string | null): string {
  if (!startDate || startDate === "Present") return "Unknown";

  const startYear = parseInt(startDate.match(/\d{4}/)?.[0] || "0");
  if (!startYear) return "Unknown";

  const currentYear = new Date().getFullYear();
  const tenure = currentYear - startYear;

  if (tenure < 1) return "Less than 1 year";
  if (tenure <= 2) return "1-2 years";
  if (tenure <= 3) return "2-3 years";
  if (tenure <= 5) return "3-5 years";
  return "5+ years";
}

/**
 * Format work history - PROFESSIONAL (no emojis)
 */
export function formatWorkHistory(workHistory: WorkHistoryItem[]): string {
  if (!workHistory || workHistory.length === 0) return "";

  return workHistory
    .map((job) => {
      const dates = `${job.start_date || "Unknown"} - ${job.end_date || "Present"}`;
      const location = job.company_location ? `\nLocation: ${job.company_location}` : "";
      const employmentType = job.employment_type ? `\nType: ${job.employment_type}` : "";

      let duties = "";
      if (job.duties_responsibilities) {
        const dutyList = job.duties_responsibilities
          .split("\n")
          .filter((d) => d.trim())
          .map((d) => `  - ${d.trim()}`)
          .join("\n");
        duties = "\nResponsibilities:\n" + dutyList;
      }

      const achievements = job.achievements
        ? `\nAchievements: ${job.achievements}`
        : "";

      const reasonLeaving = job.reason_for_leaving
        ? `\nReason for Leaving: ${job.reason_for_leaving}`
        : "";

      return `${job.job_title || "Unknown Position"}\n${job.company_name || "Unknown Company"}${location}${employmentType}\n${dates}${duties}${achievements}${reasonLeaving}`;
    })
    .join("\n\n---\n\n");
}

/**
 * Format skills - simple bullet list
 */
export function formatSkills(skills: string[]): string {
  if (!skills || skills.length === 0) return "";
  return skills.map((skill) => `- ${skill}`).join("\n");
}

/**
 * Format education - PROFESSIONAL (no emojis)
 */
export function formatEducation(education: EducationItem[]): string {
  if (!education || education.length === 0) return "";

  return education
    .map((edu) => {
      const dates = edu.end_date
        ? `Graduated: ${edu.end_date}`
        : edu.start_date
        ? `${edu.start_date} - ${edu.end_date || "Present"}`
        : "";

      const grade = edu.grade_classification ? `\nGrade: ${edu.grade_classification}` : "";
      const honours = edu.honours_awards ? `\nHonours: ${edu.honours_awards}` : "";
      const dissertation = edu.dissertation_thesis ? `\nDissertation: ${edu.dissertation_thesis}` : "";
      const extra = edu.extracurricular ? `\nActivities: ${edu.extracurricular}` : "";

      return `${edu.qualification_name || "Qualification"}\n${edu.institution_name || "Unknown Institution"}\n${dates}${grade}${honours}${dissertation}${extra}`.trim();
    })
    .join("\n\n");
}

/**
 * Format certifications - PROFESSIONAL (no emojis)
 */
export function formatCertifications(certifications: CertificationItem[]): string {
  if (!certifications || certifications.length === 0) return "";

  return certifications
    .map((cert) => {
      const issuer = cert.issuing_organisation ? `\nIssuer: ${cert.issuing_organisation}` : "";
      const obtained = cert.date_obtained ? `\nObtained: ${cert.date_obtained}` : "";
      const expiry = cert.expiry_date ? `\nExpires: ${cert.expiry_date}` : "";
      const certId = cert.certification_id ? `\nID: ${cert.certification_id}` : "";

      return `${cert.name}${issuer}${obtained}${expiry}${certId}`.trim();
    })
    .join("\n\n");
}

/**
 * Format professional memberships - PROFESSIONAL (no emojis)
 */
export function formatProfessionalMemberships(
  memberships: (ProfessionalMembershipItem | string)[]
): string {
  if (!memberships || memberships.length === 0) return "";

  if (typeof memberships[0] === "string") {
    return memberships.map((m) => `- ${m}`).join("\n");
  }

  return (memberships as ProfessionalMembershipItem[])
    .map((m) => {
      const name = m.organisation_name || "Unknown Organization";
      const type = m.membership_type ? ` (${m.membership_type})` : "";
      const since = m.member_since ? `\nMember since: ${m.member_since}` : "";
      const number = m.member_number ? `\nMember #: ${m.member_number}` : "";

      return `${name}${type}${since}${number}`.trim();
    })
    .join("\n\n");
}

/**
 * Format languages - PROFESSIONAL (no emojis)
 */
export function formatLanguages(languages: LanguageItem[]): string {
  if (!languages || languages.length === 0) return "";

  return languages
    .map((lang) => {
      const proficiency = lang.proficiency || "Unknown";
      let details = "";

      if (lang.reading || lang.writing || lang.speaking) {
        const parts = [];
        if (lang.reading) parts.push(`Reading: ${lang.reading}`);
        if (lang.writing) parts.push(`Writing: ${lang.writing}`);
        if (lang.speaking) parts.push(`Speaking: ${lang.speaking}`);
        details = `\n  ${parts.join(" | ")}`;
      }

      const certs = lang.certifications ? `\nCertifications: ${lang.certifications}` : "";

      return `${lang.language} - ${proficiency}${details}${certs}`.trim();
    })
    .join("\n\n");
}

/**
 * Format training courses - PROFESSIONAL (no emojis)
 */
export function formatTrainingCourses(courses: (TrainingCourseItem | string)[]): string {
  if (!courses || courses.length === 0) return "";

  if (typeof courses[0] === "string") {
    return courses.map((c) => `- ${c}`).join("\n");
  }

  return (courses as TrainingCourseItem[])
    .map((course) => {
      const name = course.course_name || "Training Course";
      const provider = course.provider ? `\nProvider: ${course.provider}` : "";
      const completed = course.date_completed ? `\nCompleted: ${course.date_completed}` : "";
      const duration = course.duration ? `\nDuration: ${course.duration}` : "";
      const format = course.format ? `\nFormat: ${course.format}` : "";
      const accreditation = course.accreditation ? `\nAccreditation: ${course.accreditation}` : "";

      return `${name}${provider}${completed}${duration}${format}${accreditation}`.trim();
    })
    .join("\n\n");
}

/**
 * Format volunteering and hobbies - PROFESSIONAL (no emojis)
 */
export function formatHobbiesAndVolunteering(
  volunteering: string[],
  hobbies: string[]
): string {
  let output = "";

  if (volunteering && volunteering.length > 0) {
    output += "Volunteering:\n";
    output += volunteering.map((v) => `- ${v}`).join("\n");
  }

  if (hobbies && hobbies.length > 0) {
    if (output) output += "\n\n";
    output += "Interests:\n";
    output += hobbies.map((h) => `- ${h}`).join("\n");
  }

  return output || "";
}

/**
 * Format awards and honours - PROFESSIONAL (no emojis)
 */
export function formatAwardsHonours(awards: any[]): string {
  if (!awards || awards.length === 0) return "";

  if (typeof awards[0] === "string") {
    return awards.map((a) => `- ${a}`).join("\n");
  }

  return awards
    .map((award) => {
      if (typeof award === "object") {
        const name = award.name || award.title || "Award";
        const desc = award.description ? `\n  ${award.description}` : "";
        const date = award.date ? `\n  Received: ${award.date}` : "";
        return `${name}${desc}${date}`.trim();
      }
      return `- ${award}`;
    })
    .join("\n\n");
}

/**
 * Format nationality and visa information
 */
export function formatNationalityVisa(
  nationality: string | null,
  visa: string | null
): string {
  let output = "";

  if (nationality) {
    output += `Nationality: ${nationality}`;
  }

  if (visa) {
    if (output) output += "\n";
    output += `Visa/Work Permit: ${visa}`;
  }

  return output || "Not specified";
}

/**
 * Format references - PROFESSIONAL (no emojis)
 */
export function formatReferences(references: any): string {
  if (!references) return "Not provided";

  if (typeof references === "string") {
    return references;
  }

  if (Array.isArray(references)) {
    if (references.length === 0) return "Not provided";

    if (typeof references[0] === "string") {
      return references.join("\n\n");
    }

    return references
      .map((ref, index) => {
        if (typeof ref === "object") {
          const name = ref.name || `Reference ${index + 1}`;
          const relationship = ref.relationship ? `\nRelationship: ${ref.relationship}` : "";
          const company = ref.company ? `\nCompany: ${ref.company}` : "";
          const email = ref.email ? `\nEmail: ${ref.email}` : "";
          const phone = ref.phone ? `\nPhone: ${ref.phone}` : "";

          return `${name}${relationship}${company}${email}${phone}`.trim();
        }
        return `${ref}`;
      })
      .join("\n\n");
  }

  return "Not provided";
}

/**
 * Truncate text to fit GHL field limits
 */
function truncateToLimit(text: string, maxLength: number): string {
  if (!text || text.length <= maxLength) return text;
  return text.substring(0, maxLength - 20) + "...[truncated]";
}

// ============================================
// COMPLETE GHL FIELD MAPPING - WITH 5 PARAMS
// ============================================

/**
 * Build COMPLETE GHL custom fields object with ALL 51 fields + system fields + file URLs
 * 
 * @param data - Parsed CV data
 * @param candidateId - Supabase candidate ID (for database_record_id)
 * @param cvFileUrl - URL of uploaded CV file (for candidate_provided_cv)
 * @param coverLetterUrl - URL of uploaded cover letter (for cover_letter)
 * @param otherDocsUrl - URL of uploaded other docs (for application__other_documents)
 */
export function buildCompleteGHLCustomFields(
  data: any,
  candidateId?: string,
  cvFileUrl?: string | null,
  coverLetterUrl?: string | null,
  otherDocsUrl?: string | null
): Record<string, string> {
  const fields: Record<string, string> = {};
  const config = GHL_FIELD_MAPPING.SYNC_FIELDS;
  const limits = GHL_FIELD_MAPPING.MAX_FIELD_LENGTH;

  // ============================================
  // 🔥 SYSTEM FIELDS (ALWAYS INCLUDED)
  // ============================================
  
  // Database Record ID - links to Supabase
  if (candidateId) {
    fields.database_record_id = candidateId;
  }

  // Client or Candidate - checkbox, always "true" for CV uploads (FIXED!)
  fields.client_or_candidate = "true";

  // ============================================
  // 🔥 FILE UPLOAD FIELDS
  // ============================================
  
  // Candidate Provided CV (the main CV file)
  if (cvFileUrl) {
    fields.candidate_provided_cv = cvFileUrl;
  }

  // Cover Letter
  if (coverLetterUrl) {
    fields.cover_letter = coverLetterUrl;
  }

  // Application / Other Documents (note the double underscore!)
  if (otherDocsUrl) {
    fields.application__other_documents = otherDocsUrl;
  }

  // ============================================
  // CANDIDATE DATA FIELDS
  // ============================================

  // Core identity
  if (config.cv_summary) {
    fields.cv_summary = truncateToLimit(data.cv_summary || "", limits.textarea);
  }

  // Current job info
  const currentJob = data.work_history?.[0] || {};

  if (config.current_job_title) {
    fields.current_job_title = truncateToLimit(currentJob.job_title || "", limits.text);
  }

  if (config.current_tenure) {
    fields.current_tenure = calculateTenure(currentJob.start_date);
  }

  // Work history (formatted)
  if (config.past_work_experiences) {
    fields.past_work_experiences = truncateToLimit(
      formatWorkHistory(data.work_history || []),
      limits.textarea
    );
  }

  // Skills & qualifications
  if (config.candidate_skills_summery) {
    fields.candidate_skills_summery = truncateToLimit(
      formatSkills(data.skills || []),
      limits.textarea
    );
  }

  if (config.candidate_education_history) {
    fields.candidate_education_history = truncateToLimit(
      formatEducation(data.education || []),
      limits.textarea
    );
  }

  if (config.candidate_qualifications) {
    fields.candidate_qualifications = truncateToLimit(
      formatCertifications(data.certifications || []),
      limits.textarea
    );
  }

  if (config.professional_memberships) {
    fields.professional_memberships = truncateToLimit(
      formatProfessionalMemberships(data.professional_memberships || []),
      limits.textarea
    );
  }

  // Languages & personal
  if (config.languages_spoken) {
    fields.languages_spoken = truncateToLimit(
      formatLanguages(data.languages || []),
      limits.textarea
    );
  }

  if (config.candidate_hobbies) {
    fields.candidate_hobbies = truncateToLimit(
      formatHobbiesAndVolunteering(
        data.volunteering || [],
        data.interests_hobbies || []
      ),
      limits.textarea
    );
  }

  // Career details
  if (config.future_job_aspirations) {
    fields.future_job_aspirations = truncateToLimit(
      data.future_job_aspirations || "",
      limits.textarea
    );
  }

  if (config.candidate_salary_expectation) {
    fields.candidate_salary_expectation = truncateToLimit(
      data.salary_expectation || "",
      limits.text
    );
  }

  if (config.current_notice_period) {
    fields.current_notice_period = truncateToLimit(data.notice_period || "", limits.text);
  }

  // Location & authorization
  if (config.nationality_nonbritish_visa_either_cu) {
    fields.nationality_nonbritish_visa_either_cu = truncateToLimit(
      formatNationalityVisa(data.nationality, data.visa_work_permit),
      limits.text
    );
  }

  // References
  if (config.references_contact_information) {
    fields.references_contact_information = truncateToLimit(
      formatReferences(data.candidate_references),
      limits.textarea
    );
  }

  // Additional info
  if (config.linked_in_url) {
    fields.linked_in_url = truncateToLimit(data.linkedin_url || "", limits.url);
  }

  if (config.military_experience) {
    fields.military_experience = truncateToLimit(
      data.military_service || "None",
      limits.text
    );
  }

  // Raw documents text (for AI matching later) - use documents_raw_text if available, fallback to cv_raw_text
  if (config.candidate_provided_cv_text) {
    const rawText = data.documents_raw_text || data.cv_raw_text || "";
    fields.candidate_provided_cv_text = truncateToLimit(rawText, limits.textarea);
  }

  return fields;
}

/**
 * Legacy function for backwards compatibility
 * Uses the new complete mapping internally
 */
export function buildGHLCustomFields(data: any): Record<string, string> {
  return buildCompleteGHLCustomFields(data);
}

/**
 * Get count of fields that will be synced
 */
export function getFieldSyncCount(): number {
  const config = GHL_FIELD_MAPPING.SYNC_FIELDS;
  return Object.values(config).filter((v) => v === true).length;
}

/**
 * Get list of field names that will be synced
 */
export function getSyncedFieldNames(): string[] {
  const config = GHL_FIELD_MAPPING.SYNC_FIELDS;
  return Object.keys(config).filter((k) => config[k as keyof typeof config]);
}