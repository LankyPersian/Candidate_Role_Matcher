-- ============================================
-- MIGRATION: Multi-Document Candidate Packs
-- Migration ID: 002-multidoc
-- Description: Add support for candidate packs with multiple documents (CV, cover letter, applications)
-- ============================================

-- ============================================
-- PART 1: Add columns to candidates table
-- ============================================

-- Add cover_letter_file_path column
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'candidates' 
        AND column_name = 'cover_letter_file_path'
    ) THEN
        ALTER TABLE candidates 
        ADD COLUMN cover_letter_file_path TEXT NULL;
    END IF;
END $$;

-- Add application_docs_file_paths column (JSONB array)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'candidates' 
        AND column_name = 'application_docs_file_paths'
    ) THEN
        ALTER TABLE candidates 
        ADD COLUMN application_docs_file_paths JSONB NULL;
    END IF;
END $$;

-- Add documents_raw_text column (combined text from all documents)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'candidates' 
        AND column_name = 'documents_raw_text'
    ) THEN
        ALTER TABLE candidates 
        ADD COLUMN documents_raw_text TEXT NULL;
    END IF;
END $$;

-- Add documents column (JSONB metadata about all documents in pack)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'candidates' 
        AND column_name = 'documents'
    ) THEN
        ALTER TABLE candidates 
        ADD COLUMN documents JSONB NULL;
    END IF;
END $$;

-- Ensure cv_raw_text can handle large text (should already be TEXT, but verify)
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'candidates' 
        AND column_name = 'cv_raw_text'
        AND data_type = 'character varying'
    ) THEN
        ALTER TABLE candidates 
        ALTER COLUMN cv_raw_text TYPE TEXT;
    END IF;
END $$;

-- ============================================
-- PART 2: Add columns to hold_queue table
-- ============================================

-- Add documents column (JSONB array of document metadata)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'hold_queue' 
        AND column_name = 'documents'
    ) THEN
        ALTER TABLE hold_queue 
        ADD COLUMN documents JSONB NULL;
    END IF;
END $$;

-- Add documents_raw_text column (combined text from all documents in pack)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'hold_queue' 
        AND column_name = 'documents_raw_text'
    ) THEN
        ALTER TABLE hold_queue 
        ADD COLUMN documents_raw_text TEXT NULL;
    END IF;
END $$;

-- Ensure cv_raw_text exists and is TEXT type (backward compatibility)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'hold_queue' 
        AND column_name = 'cv_raw_text'
    ) THEN
        ALTER TABLE hold_queue 
        ADD COLUMN cv_raw_text TEXT NULL;
    ELSIF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'hold_queue' 
        AND column_name = 'cv_raw_text'
        AND data_type = 'character varying'
    ) THEN
        ALTER TABLE hold_queue 
        ALTER COLUMN cv_raw_text TYPE TEXT;
    END IF;
END $$;

-- ============================================
-- PART 3: Update file_processing_status table
-- ============================================

-- Add document_type column (cv, cover_letter, application, supporting_document)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'file_processing_status' 
        AND column_name = 'document_type'
    ) THEN
        ALTER TABLE file_processing_status 
        ADD COLUMN document_type VARCHAR(50) NULL;
    END IF;
END $$;

-- Add pack_id column to link files in same candidate pack
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'file_processing_status' 
        AND column_name = 'pack_id'
    ) THEN
        ALTER TABLE file_processing_status 
        ADD COLUMN pack_id VARCHAR(255) NULL;
    END IF;
END $$;

-- ============================================
-- PART 4: Create indexes
-- ============================================

-- Index on pack_id for grouping queries
CREATE INDEX IF NOT EXISTS idx_file_status_pack_id 
ON file_processing_status(pack_id) 
WHERE pack_id IS NOT NULL;

-- Index on document_type for filtering
CREATE INDEX IF NOT EXISTS idx_file_status_document_type 
ON file_processing_status(document_type) 
WHERE document_type IS NOT NULL;

-- Index on hold_queue documents for queries
CREATE INDEX IF NOT EXISTS idx_hold_queue_documents 
ON hold_queue USING GIN (documents) 
WHERE documents IS NOT NULL;

-- Index on candidates documents for queries
CREATE INDEX IF NOT EXISTS idx_candidates_documents 
ON candidates USING GIN (documents) 
WHERE documents IS NOT NULL;

-- ============================================
-- PART 5: Add comments for documentation
-- ============================================

COMMENT ON COLUMN candidates.cover_letter_file_path IS 'Path to cover letter document in Supabase Storage';
COMMENT ON COLUMN candidates.application_docs_file_paths IS 'JSONB array of paths to application/supporting documents';
COMMENT ON COLUMN candidates.documents_raw_text IS 'Combined raw text from all documents in candidate pack (CV + cover letter + applications)';
COMMENT ON COLUMN candidates.documents IS 'JSONB metadata about all documents in pack: [{type, path, name, extracted_text_preview}]';
COMMENT ON COLUMN hold_queue.documents IS 'JSONB array of document metadata for candidate pack: [{type, path, name, extracted_text_preview}]';
COMMENT ON COLUMN hold_queue.documents_raw_text IS 'Combined raw text from all documents in pack for resume processing after manual review';
COMMENT ON COLUMN file_processing_status.document_type IS 'Type of document: cv, cover_letter, application, supporting_document, irrelevant';
COMMENT ON COLUMN file_processing_status.pack_id IS 'Identifier linking files belonging to same candidate pack';

-- ============================================
-- MIGRATION COMPLETE
-- ============================================
