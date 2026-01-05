// ============================================
// GHL DATA TRANSFORMATION FUNCTIONS (Enhanced)
// ============================================
// Converts Supabase JSON → User-friendly formatted text for GHL
// Now with COMPLETE 51-field mapping

import { GHL_FIELD_MAPPING } from "./config";

// ============================================
// INTERFACES (from original)
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
// FORMATTING FUNCTIONS (from original)
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
 * Format work history array into readable text with emojis
 */
export function formatWorkHistory(workHistory: WorkHistoryItem[]): string {
  if (!workHistory || workHistory.length === 0) return "";

  return workHistory
    .map((job) => {
      const dates = `${job.start_date || "Unknown"} - ${job.end_date || "Present"}`;
      const location = job.company_location ? `\n   📍 ${job.company_location}` : "";
      const employmentType = job.employment_type
        ? `\n   💼 ${job.employment_type}`
        : "";

      let duties = "";
      if (job.duties_responsibilities) {
        const dutyList = job.duties_responsibilities
          .split("\n")
          .filter((d) => d.trim())
          .map((d) => `   • ${d.trim()}`)
          .join("\n");
        duties = "\n\n   Key Responsibilities:\n" + dutyList;
      }

      const achievements = job.achievements
        ? `\n\n   🏆 Achievements:\n   • ${job.achievements}`
        : "";

      const reasonLeaving = job.reason_for_leaving
        ? `\n\n   📤 Reason for Leaving: ${job.reason_for_leaving}`
        : "";

      return `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📍 ${job.job_title || "Unknown Position"}
   ${job.company_name || "Unknown Company"}${location}${employmentType}
   📅 ${dates}${duties}${achievements}${reasonLeaving}`;
    })
    .join("\n\n");
}

/**
 * Format skills array into bullet list
 */
export function formatSkills(skills: string[]): string {
  if (!skills || skills.length === 0) return "";
  return skills.map((skill) => `• ${skill}`).join("\n");
}

/**
 * Format education array into readable text
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

      const grade = edu.grade_classification
        ? `\n   Grade: ${edu.grade_classification}`
        : "";

      const honours = edu.honours_awards ? `\n   Honours: ${edu.honours_awards}` : "";

      const dissertation = edu.dissertation_thesis
        ? `\n   Dissertation: ${edu.dissertation_thesis}`
        : "";

      const extra = edu.extracurricular
        ? `\n   Activities: ${edu.extracurricular}`
        : "";

      return `🎓 ${edu.qualification_name || "Qualification"}
   ${edu.institution_name || "Unknown Institution"}
   ${dates}${grade}${honours}${dissertation}${extra}`.trim();
    })
    .join("\n\n");
}

/**
 * Format certifications array into readable text
 */
export function formatCertifications(certifications: CertificationItem[]): string {
  if (!certifications || certifications.length === 0) return "";

  return certifications
    .map((cert) => {
      const issuer = cert.issuing_organisation
        ? `\n   Issuer: ${cert.issuing_organisation}`
        : "";

      const obtained = cert.date_obtained
        ? `\n   Obtained: ${cert.date_obtained}`
        : "";

      const expiry = cert.expiry_date ? `\n   Expires: ${cert.expiry_date}` : "";

      const certId = cert.certification_id ? `\n   ID: ${cert.certification_id}` : "";

      return `✅ ${cert.name}${issuer}${obtained}${expiry}${certId}`.trim();
    })
    .join("\n\n");
}

/**
 * Format professional memberships array
 */
export function formatProfessionalMemberships(
  memberships: (ProfessionalMembershipItem | string)[]
): string {
  if (!memberships || memberships.length === 0) return "";

  // If it's an array of strings, just bullet them
  if (typeof memberships[0] === "string") {
    return memberships.map((m) => `• ${m}`).join("\n");
  }

  // If it's an array of objects
  return (memberships as ProfessionalMembershipItem[])
    .map((m) => {
      const name = m.organisation_name || "Unknown Organization";
      const type = m.membership_type ? ` (${m.membership_type})` : "";
      const since = m.member_since ? `\n   Member since: ${m.member_since}` : "";
      const number = m.member_number ? `\n   Member #: ${m.member_number}` : "";

      return `🏛️ ${name}${type}${since}${number}`.trim();
    })
    .join("\n\n");
}

/**
 * Format languages array into readable text
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
        details = `\n   ${parts.join(" | ")}`;
      }

      const certs = lang.certifications
        ? `\n   Certifications: ${lang.certifications}`
        : "";

      return `🌐 ${lang.language} - ${proficiency}${details}${certs}`.trim();
    })
    .join("\n\n");
}

/**
 * Format training courses
 */
export function formatTrainingCourses(courses: (TrainingCourseItem | string)[]): string {
  if (!courses || courses.length === 0) return "";

  // If array of strings
  if (typeof courses[0] === "string") {
    return courses.map((c) => `• ${c}`).join("\n");
  }

  // If array of objects
  return (courses as TrainingCourseItem[])
    .map((course) => {
      const name = course.course_name || "Training Course";
      const provider = course.provider ? `\n   Provider: ${course.provider}` : "";
      const completed = course.date_completed
        ? `\n   Completed: ${course.date_completed}`
        : "";
      const duration = course.duration ? `\n   Duration: ${course.duration}` : "";
      const format = course.format ? `\n   Format: ${course.format}` : "";
      const accreditation = course.accreditation
        ? `\n   Accreditation: ${course.accreditation}`
        : "";

      return `📚 ${name}${provider}${completed}${duration}${format}${accreditation}`.trim();
    })
    .join("\n\n");
}

/**
 * Format volunteering and hobbies into combined text
 */
export function formatHobbiesAndVolunteering(
  volunteering: string[],
  hobbies: string[]
): string {
  let output = "";

  if (volunteering && volunteering.length > 0) {
    output += "🤝 Volunteering:\n";
    output += volunteering.map((v) => `• ${v}`).join("\n");
  }

  if (hobbies && hobbies.length > 0) {
    if (output) output += "\n\n";
    output += "🎯 Interests:\n";
    output += hobbies.map((h) => `• ${h}`).join("\n");
  }

  return output || "";
}

/**
 * Format awards and honours
 */
export function formatAwardsHonours(awards: any[]): string {
  if (!awards || awards.length === 0) return "";

  // If array of strings
  if (typeof awards[0] === "string") {
    return awards.map((a) => `🏆 ${a}`).join("\n");
  }

  // If array of objects with name/description
  return awards
    .map((award) => {
      if (typeof award === "object") {
        const name = award.name || award.title || "Award";
        const desc = award.description ? `\n   ${award.description}` : "";
        const date = award.date ? `\n   Received: ${award.date}` : "";
        return `🏆 ${name}${desc}${date}`.trim();
      }
      return `🏆 ${award}`;
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
 * Format references
 */
export function formatReferences(references: any): string {
  if (!references) return "Not provided";

  if (typeof references === "string") {
    return references;
  }

  if (Array.isArray(references)) {
    if (references.length === 0) return "Not provided";

    // If array of strings
    if (typeof references[0] === "string") {
      return references.join("\n\n");
    }

    // If array of objects
    return references
      .map((ref, index) => {
        if (typeof ref === "object") {
          const name = ref.name || `Reference ${index + 1}`;
          const relationship = ref.relationship
            ? `\n   Relationship: ${ref.relationship}`
            : "";
          const company = ref.company ? `\n   Company: ${ref.company}` : "";
          const email = ref.email ? `\n   Email: ${ref.email}` : "";
          const phone = ref.phone ? `\n   Phone: ${ref.phone}` : "";

          return `📇 ${name}${relationship}${company}${email}${phone}`.trim();
        }
        return `📇 ${ref}`;
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
// 🔥 NEW: COMPLETE GHL FIELD MAPPING
// ============================================

/**
 * Build COMPLETE GHL custom fields object with ALL 51 fields
 * Uses configuration to determine which fields to sync
 */
export function buildCompleteGHLCustomFields(data: any): Record<string, string> {
  const fields: Record<string, string> = {};
  const config = GHL_FIELD_MAPPING.SYNC_FIELDS;
  const limits = GHL_FIELD_MAPPING.MAX_FIELD_LENGTH;

  // Core identity
  if (config.cv_summary) {
    fields.cv_summary = truncateToLimit(
      data.cv_summary || "",
      limits.textarea
    );
  }

  // Current job info
  const currentJob = data.work_history?.[0] || {};

  if (config.current_job_title) {
    fields.current_job_title = truncateToLimit(
      currentJob.job_title || "",
      limits.text
    );
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
    fields.current_notice_period = truncateToLimit(
      data.notice_period || "",
      limits.text
    );
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
    fields.linked_in_url = truncateToLimit(
      data.linkedin_url || "",
      limits.url
    );
  }

  if (config.military_experience) {
    fields.military_experience = truncateToLimit(
      data.military_service || "None",
      limits.text
    );
  }

  // Raw CV text (for AI matching later)
  if (config.candidate_provided_cv_text) {
    fields.candidate_provided_cv_text = truncateToLimit(
      data.cv_raw_text || "",
      limits.textarea
    );
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
