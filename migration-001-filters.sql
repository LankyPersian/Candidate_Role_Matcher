-- ============================================
-- MIGRATION: Add Pre-Qualification Filters
-- Migration ID: 001-filters
-- Description: Add filter columns to processing_batches and ensure rejected_documents table exists
-- ============================================

-- ============================================
-- PART 1: Add columns to processing_batches table
-- ============================================

-- Add upload_type column
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'processing_batches' 
        AND column_name = 'upload_type'
    ) THEN
        ALTER TABLE processing_batches 
        ADD COLUMN upload_type VARCHAR(20) DEFAULT 'general';
    END IF;
END $$;

-- Add job_id column
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'processing_batches' 
        AND column_name = 'job_id'
    ) THEN
        ALTER TABLE processing_batches 
        ADD COLUMN job_id VARCHAR(100) NULL;
    END IF;
END $$;

-- Add required_skills column (PostgreSQL array)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'processing_batches' 
        AND column_name = 'required_skills'
    ) THEN
        ALTER TABLE processing_batches 
        ADD COLUMN required_skills TEXT[];
    END IF;
END $$;

-- Add exclude_students column
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'processing_batches' 
        AND column_name = 'exclude_students'
    ) THEN
        ALTER TABLE processing_batches 
        ADD COLUMN exclude_students BOOLEAN DEFAULT false;
    END IF;
END $$;

-- Add colleague column
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'processing_batches' 
        AND column_name = 'colleague'
    ) THEN
        ALTER TABLE processing_batches 
        ADD COLUMN colleague VARCHAR(255);
    END IF;
END $$;

-- ============================================
-- PART 2: Create indexes on processing_batches
-- ============================================

-- Index on upload_type
CREATE INDEX IF NOT EXISTS idx_batches_upload_type 
ON processing_batches(upload_type);

-- Partial index on job_id (only for non-null values)
CREATE INDEX IF NOT EXISTS idx_batches_job_id 
ON processing_batches(job_id) 
WHERE job_id IS NOT NULL;

-- ============================================
-- PART 3: Ensure rejected_documents table exists
-- ============================================

CREATE TABLE IF NOT EXISTS rejected_documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id VARCHAR(100) NOT NULL,
    file_name VARCHAR(500) NOT NULL,
    file_path TEXT NOT NULL,
    rejection_type VARCHAR(50) NOT NULL,
    rejection_reason TEXT NOT NULL,
    document_type VARCHAR(50),
    confidence FLOAT,
    classification_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================
-- PART 4: Add foreign key constraint if it doesn't exist
-- ============================================

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'rejected_documents_batch_id_fkey'
        AND table_name = 'rejected_documents'
    ) THEN
        -- Only add foreign key if processing_batches table exists
        IF EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = 'processing_batches'
        ) THEN
            ALTER TABLE rejected_documents
            ADD CONSTRAINT rejected_documents_batch_id_fkey 
            FOREIGN KEY (batch_id) REFERENCES processing_batches(id);
        END IF;
    END IF;
END $$;

-- ============================================
-- PART 5: Add missing columns to rejected_documents if table already existed
-- ============================================

-- Add rejection_type column if missing
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_name = 'rejected_documents'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'rejected_documents' 
        AND column_name = 'rejection_type'
    ) THEN
        ALTER TABLE rejected_documents 
        ADD COLUMN rejection_type VARCHAR(50) NOT NULL DEFAULT 'classification';
    END IF;
END $$;

-- ============================================
-- PART 6: Create index on rejected_documents
-- ============================================

CREATE INDEX IF NOT EXISTS idx_rejected_docs_batch 
ON rejected_documents(batch_id);

-- ============================================
-- MIGRATION COMPLETE
-- ============================================
