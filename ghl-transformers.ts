// ============================================
// GHL DATA TRANSFORMATION FUNCTIONS (TypeScript)
// ============================================
// Converts Supabase JSON → User-friendly formatted text for GHL

interface WorkHistoryItem {
  job_title?: string;
  company_name?: string;
  company_location?: string;
  start_date?: string;
  end_date?: string;
  duties_responsibilities?: string;
  achievements?: string;
}

interface EducationItem {
  qualification_name?: string;
  institution_name?: string;
  start_date?: string;
  end_date?: string;
  grade_classification?: string;
  honours_awards?: string;
  dissertation_thesis?: string;
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
}

/**
 * Split full name into first and last name
 */
export function splitName(fullName: string | null): { firstName: string; lastName: string } {
  if (!fullName) return { firstName: '', lastName: '' };
  
  const parts = fullName.trim().split(' ');
  if (parts.length === 1) {
    return { firstName: parts[0], lastName: '' };
  }
  
  const lastName = parts.pop()!;
  const firstName = parts.join(' ');
  
  return { firstName, lastName };
}

/**
 * Calculate tenure category from work start date
 */
export function calculateTenure(startDate: string | null): string {
  if (!startDate || startDate === 'Present') return 'Unknown';
  
  const startYear = parseInt(startDate.match(/\d{4}/)?.[0] || '0');
  if (!startYear) return 'Unknown';
  
  const currentYear = new Date().getFullYear();
  const tenure = currentYear - startYear;
  
  if (tenure < 1) return 'Less than 1 year';
  if (tenure <= 2) return '1-2 years';
  if (tenure <= 3) return '2-3 years';
  if (tenure <= 5) return '3-5 years';
  return '5+ years';
}

/**
 * Format work history array into readable text with emojis
 */
export function formatWorkHistory(workHistory: WorkHistoryItem[]): string {
  if (!workHistory || workHistory.length === 0) return '';
  
  return workHistory.map(job => {
    const dates = `${job.start_date || 'Unknown'} - ${job.end_date || 'Present'}`;
    const location = job.company_location ? `\n   📍 ${job.company_location}` : '';
    
    let duties = '';
    if (job.duties_responsibilities) {
      const dutyList = job.duties_responsibilities
        .split('\n')
        .filter(d => d.trim())
        .map(d => `   • ${d.trim()}`)
        .join('\n');
      duties = '\n\n   Key Responsibilities:\n' + dutyList;
    }
    
    const achievements = job.achievements 
      ? `\n\n   🏆 Achievements:\n   • ${job.achievements}` 
      : '';
    
    return `━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📍 ${job.job_title || 'Unknown Position'}
   ${job.company_name || 'Unknown Company'}${location}
   📅 ${dates}${duties}${achievements}`;
  }).join('\n\n');
}

/**
 * Format skills array into bullet list
 */
export function formatSkills(skills: string[]): string {
  if (!skills || skills.length === 0) return '';
  return skills.map(skill => `• ${skill}`).join('\n');
}

/**
 * Format education array into readable text
 */
export function formatEducation(education: EducationItem[]): string {
  if (!education || education.length === 0) return '';
  
  return education.map(edu => {
    const dates = edu.end_date 
      ? `Graduated: ${edu.end_date}`
      : edu.start_date 
        ? `${edu.start_date} - ${edu.end_date || 'Present'}`
        : '';
    
    const grade = edu.grade_classification 
      ? `\n   Grade: ${edu.grade_classification}` 
      : '';
    
    const honours = edu.honours_awards 
      ? `\n   Honours: ${edu.honours_awards}` 
      : '';
    
    const dissertation = edu.dissertation_thesis
      ? `\n   Dissertation: ${edu.dissertation_thesis}`
      : '';
    
    return `🎓 ${edu.qualification_name || 'Qualification'}
   ${edu.institution_name || 'Unknown Institution'}
   ${dates}${grade}${honours}${dissertation}`.trim();
  }).join('\n\n');
}

/**
 * Format certifications array into readable text
 */
export function formatCertifications(certifications: CertificationItem[]): string {
  if (!certifications || certifications.length === 0) return '';
  
  return certifications.map(cert => {
    const issuer = cert.issuing_organisation 
      ? `\n   Issuer: ${cert.issuing_organisation}` 
      : '';
    
    const obtained = cert.date_obtained 
      ? `\n   Obtained: ${cert.date_obtained}` 
      : '';
    
    const expiry = cert.expiry_date 
      ? `\n   Expires: ${cert.expiry_date}` 
      : '';
    
    const certId = cert.certification_id
      ? `\n   ID: ${cert.certification_id}`
      : '';
    
    return `✅ ${cert.name}${issuer}${obtained}${expiry}${certId}`.trim();
  }).join('\n\n');
}

/**
 * Format professional memberships array
 */
export function formatProfessionalMemberships(memberships: any[]): string {
  if (!memberships || memberships.length === 0) return '';
  
  // If it's an array of strings, just bullet them
  if (typeof memberships[0] === 'string') {
    return memberships.map(m => `• ${m}`).join('\n');
  }
  
  // If it's an array of objects with organisation_name
  return memberships.map(m => `• ${m.organisation_name || m}`).join('\n');
}

/**
 * Format languages array into readable text
 */
export function formatLanguages(languages: LanguageItem[]): string {
  if (!languages || languages.length === 0) return '';
  
  return languages.map(lang => {
    const proficiency = lang.proficiency || 'Unknown';
    return `🌐 ${lang.language} - ${proficiency}`;
  }).join('\n');
}

/**
 * Format volunteering and hobbies into combined text
 */
export function formatHobbiesAndVolunteering(
  volunteering: string[], 
  hobbies: string[]
): string {
  let output = '';
  
  if (volunteering && volunteering.length > 0) {
    output += '🤝 Volunteering:\n';
    output += volunteering.map(v => `• ${v}`).join('\n');
  }
  
  if (hobbies && hobbies.length > 0) {
    if (output) output += '\n\n';
    output += '🎯 Interests:\n';
    output += hobbies.map(h => `• ${h}`).join('\n');
  }
  
  return output || '';
}

/**
 * Format nationality and visa information
 */
export function formatNationalityVisa(
  nationality: string | null, 
  visa: string | null
): string {
  let output = '';
  
  if (nationality) {
    output += `Nationality: ${nationality}`;
  }
  
  if (visa) {
    if (output) output += '\n';
    output += `Visa/Work Permit: ${visa}`;
  }
  
  return output || 'Not specified';
}

/**
 * Format references
 */
export function formatReferences(references: any): string {
  if (!references) return 'Not provided';
  
  if (typeof references === 'string') {
    return references;
  }
  
  if (Array.isArray(references)) {
    if (references.length === 0) return 'Not provided';
    return references.join('\n\n');
  }
  
  return 'Not provided';
}

/**
 * Build GHL custom fields object with formatted data
 */
export function buildGHLCustomFields(data: any): Record<string, string> {
  const currentJob = data.work_history?.[0] || {};
  
  return {
    // Job info
    current_job_title: currentJob.job_title || '',
    current_tenure: calculateTenure(currentJob.start_date),
    
    // Detailed info (formatted as user-friendly text)
    past_work_experiences: formatWorkHistory(data.work_history || []),
    candidate_skills_summery: formatSkills(data.skills || []), // Note: typo in GHL field name
    candidate_education_history: formatEducation(data.education || []),
    candidate_qualifications: formatCertifications(data.certifications || []),
    professional_memberships: formatProfessionalMemberships(data.professional_memberships || []),
    
    // Additional info
    languages_spoken: formatLanguages(data.languages || []),
    candidate_hobbies: formatHobbiesAndVolunteering(
      data.volunteering || [], 
      data.interests_hobbies || []
    ),
    future_job_aspirations: data.future_job_aspirations || '',
    nationality_nonbritish_visa_either_cu: formatNationalityVisa(
      data.nationality,
      data.visa_work_permit
    ),
    references_contact_information: formatReferences(data.candidate_references),
    
    // Links and documents
    linked_in_url: data.linkedin_url || '',
    candidate_provided_cv_text: data.cv_raw_text || '',
    
    // Expectations
    candidate_salary_expectation: data.salary_expectation || '',
    current_notice_period: data.notice_period || '',
    
    // Other
    military_experience: data.military_service || 'None',
    
    // AI summary
    cv_summary: data.cv_summary || ''
  };
}