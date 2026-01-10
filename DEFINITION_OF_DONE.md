# Definition of Done - CV Processor V2

## Overview
The CV Processor V2 must handle mixed file uploads (CVs, cover letters, application documents) in batches, group them into candidate packs, and process each pack as a cohesive unit.

---

## Core Requirements Checklist

### 1. Batch Upload & File Grouping
- [ ] Recruiter can upload multiple mixed files (CVs, cover letters, application docs) in a single batch
- [ ] System groups uploaded files into candidate packs based on:
  - Filename patterns (e.g., `JohnDoe_CV.pdf`, `JohnDoe_CoverLetter.pdf`)
  - Name extraction from initial quick parse
  - Manual grouping via UI if needed
- [ ] Each candidate pack contains:
  - One primary CV document (required)
  - Zero or more cover letters (optional)
  - Zero or more other application documents (optional)
- [ ] Files that cannot be grouped remain as standalone items for manual review

### 2. Pack-Level Processing
- [ ] System processes each candidate pack as a unit (not individual files)
- [ ] All documents in a pack are parsed together to extract complete candidate data
- [ ] System merges data from all pack documents (CV + cover letter + other docs)
- [ ] Raw text from ALL documents in the pack is stored in Supabase `candidates.cv_raw_text` (concatenated or structured)

### 3. Supabase Candidate Population
- [ ] All candidate columns are populated from parsed pack data:
  - Core fields: `full_name`, `email`, `phone`, `address`, `linkedin_url`
  - Professional: `professional_summary`, `work_history`, `education`, `skills`
  - Additional: `certifications`, `languages`, `salary_expectation`, `notice_period`, etc.
  - Metadata: `batch_id`, `client_id`, `cv_file_path`, `status`
- [ ] `cv_raw_text` contains text from all documents in the pack (for future job matching)
- [ ] Candidate record links to all pack documents via `cv_file_path` and associated file references

### 4. GoHighLevel File Uploads
- [ ] CV file uploaded to GHL custom field: `candidate_provided_cv`
- [ ] Cover letter uploaded to GHL custom field: `cover_letter` (if exists in pack)
- [ ] Other application documents uploaded to GHL custom field: `application__other_documents` (if exists in pack)
- [ ] File URLs are stored in GHL contact custom fields (not just Supabase)
- [ ] All files are uploaded to GHL `/medias/upload-file` endpoint and URLs retrieved

### 5. GHL Contact Linking
- [ ] `database_record_id` custom field in GHL contact contains Supabase candidate UUID
- [ ] Link is bidirectional: Supabase `candidates.ghl_contact_id` also populated
- [ ] Existing contacts are found via email/phone matching and updated (not duplicated)
- [ ] New contacts are created with all 51 custom fields populated

### 6. Hold Queue Management
- [ ] Missing contact info (no email AND no phone) → sent to `hold_queue` table
- [ ] Duplicate detection (email or phone match in Supabase/GHL) → sent to `hold_queue`
- [ ] Hold queue items can be resolved from UI with:
  - Manual contact info entry (full_name, email, phone)
  - Duplicate resolution (update existing vs create new)
  - Status update to `ready_for_processing` triggers `processHoldQueueItem` task
- [ ] Resolved hold queue items complete processing with full pack support

### 7. Status Tracking
- [ ] `file_processing_status` table tracks:
  - Individual file status: `pending`, `processing`, `complete`, `failed`, `rejected`
  - Pack association (which pack a file belongs to)
  - Candidate ID (once pack is processed)
- [ ] `processing_batches` table tracks:
  - Batch status: `pending`, `processing`, `complete`, `awaiting_input`, `failed`
  - `processed_count` increments per pack (not per file)
  - `completed_at` set when batch finishes
- [ ] UI can query both tables to display real-time progress
- [ ] Real-time updates via Supabase Realtime subscriptions work correctly

### 8. Error Handling & Edge Cases
- [ ] Invalid files (non-CVs, corrupted, too large) are rejected and logged
- [ ] Partially complete packs (CV missing) are sent to hold queue
- [ ] Failed GHL uploads don't block Supabase save (data is safe)
- [ ] Batch recovery handles timeouts and retries correctly
- [ ] File grouping failures default to single-file processing with UI review option

---

## Success Criteria

### Functional
- ✅ Upload 15 mixed files → System groups into ~5 candidate packs
- ✅ Each pack processed → 1 candidate record + 1 GHL contact + all files uploaded
- ✅ Missing info → Appears in hold queue → Resolved from UI → Completes processing
- ✅ Duplicates → Detected → Sent to hold queue → Resolved → Updates existing or creates new
- ✅ All status updates visible in real-time UI

### Technical
- ✅ All candidate data fields populated in Supabase
- ✅ All file uploads successful in GHL (CV, cover letter, other docs)
- ✅ `database_record_id` links GHL → Supabase
- ✅ `cv_raw_text` contains all pack document text
- ✅ Status tables accurate and queryable by UI
- ✅ No data loss on GHL failures (Supabase is source of truth)

---

## Testing Checklist

### Unit Tests
- [ ] File grouping algorithm (name-based, pattern matching)
- [ ] Pack processing logic (merge data from multiple docs)
- [ ] GHL file upload functions (CV, cover letter, other docs)
- [ ] Duplicate detection (Supabase + GHL)
- [ ] Status update functions

### Integration Tests
- [ ] Full pack processing flow (upload → group → parse → save → GHL sync)
- [ ] Hold queue resolution flow (UI update → task trigger → completion)
- [ ] Batch status transitions (pending → processing → complete/awaiting_input)
- [ ] Real-time subscription updates in UI

### Manual Testing
- [ ] Upload batch with mixed files → Verify grouping
- [ ] Verify all pack documents uploaded to correct GHL fields
- [ ] Test hold queue resolution from UI
- [ ] Verify status updates appear in real-time
- [ ] Test duplicate detection and resolution

---

## Definition of Done - Final Checklist

Before marking this feature complete, verify:

1. [ ] All core requirements implemented
2. [ ] All files listed in "Files to Change" are updated
3. [ ] Database schema supports pack grouping (if needed)
4. [ ] UI displays pack information and status correctly
5. [ ] Error handling covers all edge cases
6. [ ] Real-time updates work in production UI
7. [ ] GHL sync is complete and bidirectional
8. [ ] Hold queue resolution flow is functional
9. [ ] Documentation updated (if applicable)
10. [ ] No breaking changes to existing functionality

---

## Notes

- **File Grouping Strategy**: Implement intelligent grouping using filename patterns, extracted names, and metadata. Fallback to manual grouping if automated grouping fails.
- **Pack Structure**: Store pack metadata in database (new table or JSON in existing table) to track which files belong together.
- **Raw Text Storage**: Concatenate all pack document text or store as structured JSON in `cv_raw_text` for future job matching.
- **Status Granularity**: Track both file-level and pack-level status for better UI visibility.
