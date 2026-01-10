// ============================================
// GHL FIELD MAPPING HELPER
// ============================================
// Fetches GHL custom fields and maps field keys to IDs
// Handles both key-based and ID-based field updates

import { logger } from "@trigger.dev/sdk";
import { ENV, GHL_CONFIG, getRetryDelay, isRetryableStatus } from "./config";

// ============================================
// TYPES
// ============================================

interface GHLCustomField {
  id?: string;
  key?: string;
  name: string;
  fieldType: string;
  dataType: string;
}

interface GHLFieldMapping {
  keyToId: Map<string, string>;
  idToKey: Map<string, string>;
  fields: GHLCustomField[];
  fetchedAt: number;
}

// Cache field mapping for a short time to avoid repeated API calls
let fieldMappingCache: GHLFieldMapping | null = null;
const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

// ============================================
// FETCH CUSTOM FIELDS
// ============================================

/**
 * Fetch all custom fields from GHL API
 */
async function fetchGHLCustomFields(accessToken: string): Promise<GHLCustomField[]> {
  const url = `${GHL_CONFIG.BASE_URL}/custom-fields/`;
  const maxAttempts = GHL_CONFIG.MAX_RETRIES;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const response = await fetch(url, {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          Version: GHL_CONFIG.API_VERSION,
        },
      });

      if (response.ok) {
        const data = await response.json();
        // GHL API may return fields in different structures
        const fields = data.customFields || data.fields || data || [];
        return Array.isArray(fields) ? fields : [];
      }

      const status = response.status;
      const retryable = isRetryableStatus(status, GHL_CONFIG.RETRYABLE_STATUS_CODES);

      if (!retryable || attempt === maxAttempts) {
        const errorText = await response.text();
        throw new Error(`GHL custom fields fetch failed (status ${status}): ${errorText.slice(0, 500)}`);
      }

      const waitMs = getRetryDelay(attempt, GHL_CONFIG);
      logger.warn("Retrying GHL custom fields fetch", { attempt, waitMs });
      await new Promise((resolve) => setTimeout(resolve, waitMs));
    } catch (error: any) {
      if (attempt === maxAttempts) {
        throw error;
      }
      logger.warn("GHL custom fields fetch exception", {
        attempt,
        error: error.message,
      });
      const waitMs = getRetryDelay(attempt, GHL_CONFIG);
      await new Promise((resolve) => setTimeout(resolve, waitMs));
    }
  }

  throw new Error("GHL custom fields fetch failed after retries");
}

/**
 * Build field mapping from fetched fields
 */
function buildFieldMapping(fields: GHLCustomField[]): GHLFieldMapping {
  const keyToId = new Map<string, string>();
  const idToKey = new Map<string, string>();

  for (const field of fields) {
    // GHL may use either 'key' or 'id' as identifier
    const identifier = field.key || field.id;
    const id = field.id || field.key;

    if (identifier && id) {
      keyToId.set(identifier, id);
      idToKey.set(id, identifier);
    }

    // Also map by name (lowercase, normalized) as fallback
    if (field.name && id) {
      const normalizedName = field.name.toLowerCase().replace(/[^a-z0-9]/g, "_");
      if (!keyToId.has(normalizedName)) {
        keyToId.set(normalizedName, id);
      }
    }
  }

  return {
    keyToId,
    idToKey,
    fields,
    fetchedAt: Date.now(),
  };
}

/**
 * Get or fetch GHL field mapping (with caching)
 */
export async function getGHLFieldMapping(accessToken: string): Promise<GHLFieldMapping> {
  // Check cache
  if (fieldMappingCache && Date.now() - fieldMappingCache.fetchedAt < CACHE_TTL_MS) {
    return fieldMappingCache;
  }

  // Fetch from API
  logger.info("Fetching GHL custom fields mapping");
  const fields = await fetchGHLCustomFields(accessToken);
  const mapping = buildFieldMapping(fields);

  // Update cache
  fieldMappingCache = mapping;

  logger.info("GHL field mapping updated", {
    total_fields: fields.length,
    mapped_keys: mapping.keyToId.size,
  });

  return mapping;
}

/**
 * Convert field key to ID using mapping
 */
export async function getGHLFieldId(
  fieldKey: string,
  accessToken: string
): Promise<string | null> {
  const mapping = await getGHLFieldMapping(accessToken);
  return mapping.keyToId.get(fieldKey) || null;
}

/**
 * Convert custom fields object to GHL API format (with ID mapping)
 */
export async function mapFieldsToGHLFormat(
  customFields: Record<string, string>,
  accessToken: string
): Promise<Array<{ key?: string; id?: string; value: string }>> {
  const mapping = await getGHLFieldMapping(accessToken);
  const mappedFields: Array<{ key?: string; id?: string; value: string }> = [];

  for (const [key, value] of Object.entries(customFields)) {
    const fieldId = mapping.keyToId.get(key);
    
    if (fieldId) {
      // Use ID if available
      mappedFields.push({ id: fieldId, value: value || "" });
    } else {
      // Fall back to key (GHL may accept both)
      mappedFields.push({ key, value: value || "" });
      logger.warn("GHL field key not found in mapping, using key directly", {
        fieldKey: key,
        totalMappedFields: mapping.keyToId.size,
      });
    }
  }

  return mappedFields;
}

/**
 * Clear field mapping cache (useful for testing or forced refresh)
 */
export function clearFieldMappingCache(): void {
  fieldMappingCache = null;
  logger.info("GHL field mapping cache cleared");
}
