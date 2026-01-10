# Files That Must Change - CV Processor V2

## Overview
This document lists all files that need modification to implement candidate pack grouping and processing.

---

## Backend Files (TypeScript/Trigger.dev)

### 1. `src/trigger/processCVBatch.ts` ⚠️ **MAJOR CHANGES**
**Current State**: Processes files individually, no grouping logic

**Changes Required**:
- Add file grouping function: `groupFilesIntoPacks(files: File[]): CandidatePack[]`
  - Group by filename patterns (e.g., `JohnDoe_CV.pdf`, `JohnDoe_CoverLetter.pdf`)
  - Group by extracted name from quick parse
  - Handle edge cases (orphaned files, ambiguous groups)
- Modify main processing loop to iterate over packs instead of files
- Update `listBatchFiles()` to return all files (including subfolders for cover letters/other docs)
- Change processing logic to:
  - Process all documents in pack together
  - Merge parsed data from CV + cover letter + other docs
  - Store concatenated raw text from all pack documents
- Update status tracking to track pack-level progress
- Modify GHL upload logic to identify document type (CV vs cover letter vs other) and upload to correct field

**Key Functions to Modify/Add**:
- `groupFilesIntoPacks()` - NEW
- `processCandidatePack()` - NEW (extracted from main loop)
- `parsePackDocuments()` - NEW (parses all docs in pack)
- `mergePackData()` - NEW (merges CV + cover letter + other docs data)
- `uploadPackFilesToGHL()` - MODIFY (identify doc type, upload to correct field)
- Main `run()` function - MODIFY (iterate packs, not files)

---

### 2. `src/trigger/ghlTransformers.ts` ⚠️ **MODERATE CHANGES**
**Current State**: Handles single CV file upload

**Changes Required**:
- Update `buildCompleteGHLCustomFields()` to accept pack file URLs:
  - `cvFileUrl` → `candidate_provided_cv`
  - `coverLetterUrl` → `cover_letter`
  - `otherDocsUrl` → `application__other_documents` (note: double underscore)
- Ensure `database_record_id` is always set with Supabase candidate UUID
- Verify all 51 custom fields are populated correctly

**Key Functions to Modify**:
- `buildCompleteGHLCustomFields()` - MODIFY (already has params, but needs to ensure correct mapping)

---

### 3. `src/trigger/processCVBatch.ts` - GHL Upload Functions ⚠️ **MODERATE CHANGES**
**Current State**: `uploadCoverLetterToGHL()` and `uploadOtherDocsToGHL()` search for files in subfolders

**Changes Required**:
- Modify `uploadCVToGHL()` - ensure it's called with correct pack CV file
- Update `uploadCoverLetterToGHL()` - use pack structure instead of folder guessing
- Update `uploadOtherDocsToGHL()` - use pack structure instead of folder guessing
- Add function to determine document type from pack metadata:
  - `getDocumentTypeFromPack(fileName: string, pack: CandidatePack): 'cv' | 'cover_letter' | 'other'`

**Key Functions to Modify**:
- `uploadCVToGHL()` - MODIFY (use pack CV file path)
- `uploadCoverLetterToGHL()` - MODIFY (use pack cover letter file path if exists)
- `uploadOtherDocsToGHL()` - MODIFY (use pack other docs file paths if exist)

---

### 4. `src/trigger/processHoldQueueItem.ts` ⚠️ **MODERATE CHANGES**
**Current State**: Processes single file from hold queue

**Changes Required**:
- Update to handle candidate packs (not just single files)
- If hold queue item is part of a pack, process entire pack after resolution
- Ensure pack files are uploaded to correct GHL fields after resolution
- Update duplicate resolution to handle pack-level duplicates

**Key Functions to Modify**:
- Main `run()` function - MODIFY (check if item is part of pack, process pack if so)
- `uploadPackFilesToGHL()` - ADD (similar to processCVBatch version)

---

### 5. `src/trigger/config.ts` ⚠️ **MINOR CHANGES**
**Current State**: Configuration constants

**Changes Required**:
- Add configuration for file grouping:
  - `FILE_GROUPING_PATTERNS` - regex patterns for CV, cover letter, other docs
  - `PACK_GROUPING_STRATEGY` - name-based, pattern-based, or manual
  - `MAX_PACK_SIZE` - maximum files per pack
- Add validation rules for pack structure

**Key Additions**:
- `FILE_GROUPING_CONFIG` - NEW
- `PACK_VALIDATION_RULES` - NEW

---

## Frontend Files (HTML/JavaScript)

### 6. `Demo_HTML_Files/cv-upload-production.html` ⚠️ **MODERATE CHANGES**
**Current State**: Uploads files individually, displays individual file status

**Changes Required**:
- Update UI to display candidate packs instead of individual files
- Show pack structure (CV + cover letter + other docs) in upload summary
- Update real-time subscriptions to handle pack-level updates
- Modify review queue to show packs (not individual files)
- Update progress tracking to show pack count vs file count
- Add UI for manual pack grouping if automated grouping fails

**Key Functions to Modify**:
- `subscribeToRealtimeUpdates()` - MODIFY (handle pack-level updates)
- `renderSuccessTable()` - MODIFY (display packs)
- `renderReviewQueue()` - MODIFY (display packs in hold queue)
- File upload display logic - MODIFY (group files into packs visually)

---

### 7. `HTML_web_applications/CV_uploader.html` ⚠️ **MODERATE CHANGES**
**Current State**: Similar to production HTML file

**Changes Required**:
- Same changes as `cv-upload-production.html`
- Ensure consistency with production version

---

## Database Schema (Migration Files)

### 8. `migration-001-filters.sql` or NEW migration ⚠️ **NEW FILE NEEDED**
**Current State**: Existing migrations may not support pack grouping

**Changes Required**:
- Create new migration file: `migration-002-candidate-packs.sql`
- Add `candidate_packs` table (optional, if storing pack metadata):
  ```sql
  CREATE TABLE candidate_packs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id UUID REFERENCES processing_batches(id),
    client_id UUID,
    pack_name VARCHAR(255),
    cv_file_path TEXT,
    cover_letter_file_path TEXT[],
    other_docs_file_path TEXT[],
    status VARCHAR(50),
    candidate_id UUID REFERENCES candidates(id),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
  );
  ```
- OR: Add pack metadata columns to existing `file_processing_status` table:
  - `pack_id UUID` - links files in same pack
  - `document_type VARCHAR(50)` - 'cv', 'cover_letter', 'other'
- Update `candidates` table if needed:
  - Ensure `cv_raw_text` can store large text (TEXT type, not VARCHAR)
  - Add `cover_letter_file_path TEXT[]` if storing multiple
  - Add `other_docs_file_path TEXT[]` if storing multiple

**Key Tables to Modify/Create**:
- `candidate_packs` - NEW (optional)
- `file_processing_status` - MODIFY (add pack_id, document_type)
- `candidates` - MODIFY (ensure cv_raw_text supports large text, add file path arrays)

---

## Utility/Helper Files

### 9. `src/trigger/documentClassifier.ts` ⚠️ **MINOR CHANGES**
**Current State**: Classifies single document type

**Changes Required**:
- Add function to identify document type within a pack:
  - `identifyDocumentType(fileName: string, rawText: string): 'cv' | 'cover_letter' | 'application' | 'other'`
- Use filename patterns and text analysis to determine type
- This helps route documents to correct GHL fields

**Key Functions to Add**:
- `identifyDocumentType()` - NEW

---

## Configuration & Constants

### 10. No additional config files needed
All configuration should go into `src/trigger/config.ts` (see #5)

---

## Summary of File Changes

| File | Change Type | Priority | Effort |
|------|------------|----------|--------|
| `src/trigger/processCVBatch.ts` | MAJOR | HIGH | Large |
| `src/trigger/ghlTransformers.ts` | MODERATE | HIGH | Medium |
| `src/trigger/processHoldQueueItem.ts` | MODERATE | HIGH | Medium |
| `src/trigger/config.ts` | MINOR | MEDIUM | Small |
| `src/trigger/documentClassifier.ts` | MINOR | MEDIUM | Small |
| `Demo_HTML_Files/cv-upload-production.html` | MODERATE | HIGH | Medium |
| `HTML_web_applications/CV_uploader.html` | MODERATE | MEDIUM | Medium |
| `migration-002-candidate-packs.sql` | NEW | HIGH | Medium |

**Total Files to Change**: 8 files (7 existing + 1 new migration)

---

## Implementation Order (Recommended)

1. **Database Schema** (Migration file)
   - Define pack structure
   - Update tables

2. **Backend Core Logic** (`processCVBatch.ts`)
   - File grouping function
   - Pack processing logic
   - Status tracking updates

3. **GHL Integration** (`ghlTransformers.ts`, upload functions)
   - Update file upload logic
   - Correct field mapping

4. **Hold Queue** (`processHoldQueueItem.ts`)
   - Pack support in hold queue

5. **Frontend** (HTML files)
   - UI updates for pack display
   - Real-time subscription updates

6. **Testing & Refinement**
   - Unit tests
   - Integration tests
   - Manual testing

---

## Breaking Changes

⚠️ **Potential Breaking Changes**:
- Status tracking now uses pack-level granularity (may affect existing UI queries)
- File structure in storage may need reorganization (subfolders for packs)
- Hold queue items now reference packs (not just single files)
- API responses may return pack structure instead of individual files

**Migration Path**:
- Keep backward compatibility for single-file processing during transition
- Add feature flag to enable pack processing
- Gradual rollout with fallback to individual file processing
