/**
* HubSpot Workflow Action: Prepare Event Associations
* -----------------------------------------------
* For Workflow: [Events] | 06 Create Associations | Prepare Association Lists
* 
* Description:
* Fetches all registrations for an event from ConnectSpace, finds corresponding
* HubSpot contacts and companies, and prepares association records for processing.
* 
* Required Secrets:
* - HUBSPOT_ACCESS_TOKEN: HubSpot Private App Access Token
* - CONNECT_SPACE_API_KEY: ConnectSpace API Key
* - CONNECT_SPACE_BASE_URL: ConnectSpace API Base URL
* - HUBSPOT_EVENT_OBJECT_ID: HubSpot Custom Object ID for Events
* 
* Input Properties:
* - hs_object_id: String (HubSpot Event ID)
* - cs_event_id: String (ConnectSpace Event ID)
* - cs_processing_queue_data: String (Queue data from previous step)
* 
* Output Properties:
* - cs_processing_queue_status: String (Next workflow step or error)
* - cs_processing_queue_data: String (Queue data for next step)
*/

const hubspot = require('@hubspot/api-client');
const axios = require('axios');

// Workflow identification constant
const WORKFLOW_NAME = "06_ASC_PREPARE";

/**
 * Best-effort display name builder for ConnectSpace/HubSpot objects.
 * ConnectSpace commonly uses `full_name`, but older code paths used `fullname`.
 */
function getDisplayName(data) {
    if (!data || typeof data !== 'object') return null;

    const directCandidates = [
        data.name,
        data.full_name,
        data.fullname,
        data.fullName,
        data.cs_full_name
    ].filter(v => typeof v === 'string' && v.trim());

    if (directCandidates.length > 0) return directCandidates[0].trim();

    const first = (data.first_name ?? data.firstname ?? data.firstName ?? '').toString().trim();
    const last = (data.last_name ?? data.lastname ?? data.lastName ?? '').toString().trim();
    const combined = `${first} ${last}`.trim();
    return combined || null;
}

// Embedded Error Manager - Self-contained error tracking
class ErrorManager {
    constructor(hubspotClient, objectType, objectId, previousErrorLogs = null, errorExists = false) {
        this.hubspotClient = hubspotClient;
        this.objectType = objectType;
        this.objectId = objectId;
        this.previousErrorLogs = previousErrorLogs; // Previous errors from input property
        this.errorExists = errorExists; // Flag indicating if previous errors exist
        this.errors = {};
        this.errorSummary = { total_errors: 0, critical_errors: 0, retryable_errors: 0, non_retryable_errors: 0, error_distribution: {} };
        // First-error-wins per email (within a single run)
        this._seenEmails = new Set();
    }

    async loadErrors() {
        // Skip loading at initialization - only load when we have errors to save
        this.errors = {};
    }

    categorizeError(error, context = {}) {
        const msg = (error?.message || String(error || '')).toLowerCase();
        if (error?.response?.status || error?.statusCode || msg.includes('timeout') || msg.includes('rate limit') || msg.includes('network') || context.apiEndpoint) return 'API_ERROR';
        if (msg.includes('missing required environment') || msg.includes('environment variable')) return 'ENVIRONMENT_ERROR';
        if (msg.includes('queue') || msg.includes('invalid queue') || msg.includes('corrupted json') || context.queueRelated) return 'QUEUE_MANAGEMENT_ERROR';
        if (msg.includes('duplicate') || msg.includes('already exists') || error?.statusCode === 409) return 'DUPLICATE_ERROR';
        // Validation errors (email/phone/etc). Do NOT use `context.field` alone, because it is also used for missing/lookup failures.
        const validationFields = new Set(['email', 'phone', 'mobile', 'mobilephone']);
        if (
            msg.includes('invalid email') ||
            msg.includes('malformed') ||
            msg.includes('invalid format') ||
            msg.includes('mx record') ||
            msg.includes('mx records') ||
            msg.includes('no mx') ||
            (context.field && validationFields.has(String(context.field).toLowerCase()))
        ) return 'VALIDATION_ERROR';
        if (msg.includes('missing') || msg.includes('required field') || msg.includes('not found')) return 'MISSING_DATA';
        if (msg.includes('event') && (msg.includes('invalid date') || msg.includes('date'))) return 'EVENT_VALIDATION_ERROR';
        if (msg.includes('integration') || msg.includes('sync') || context.integration) return 'INTEGRATION_ERROR';
        return 'SYSTEM_ERROR';
    }

    logError(error, itemData = {}, context = {}) {
        const errorType = this.categorizeError(error, context);
        if (!this.errors[errorType]) this.errors[errorType] = { count: 0, items: [] };

        // If this email has already errored in this run, ignore subsequent errors for it
        const emailKey = (typeof itemData.email === 'string' && itemData.email.trim())
            ? itemData.email.trim().toLowerCase()
            : null;
        if (emailKey && this._seenEmails?.has(emailKey)) return;

        const derivedName = getDisplayName(itemData);
        const errorItem = {
            id: itemData.id || itemData.accountId || itemData.eventId || itemData.registrationId || 'unknown',
            timestamp: new Date().toISOString(),
            error: error?.message || String(error || 'Unknown error'),
            workflow: WORKFLOW_NAME
        };

        if (errorType === 'VALIDATION_ERROR') {
            if (itemData.email) errorItem.email = itemData.email;
            if (itemData.field) errorItem.field = itemData.field;
            if (itemData.name) errorItem.name = itemData.name;
        } else if (errorType === 'API_ERROR') {
            if (itemData.apiEndpoint) errorItem.api_endpoint = itemData.apiEndpoint;
            if (error?.response?.status || error?.statusCode) errorItem.status_code = error.response?.status || error.statusCode;
            if (error?.code) errorItem.error_code = error.code;
        } else if (errorType === 'MISSING_DATA') {
            if (itemData.field) errorItem.field = itemData.field;
            if (itemData.email) errorItem.email = itemData.email;
        } else if (errorType === 'DUPLICATE_ERROR') {
            if (itemData.email) errorItem.email = itemData.email;
            if (itemData.duplicateId) errorItem.duplicate_id = itemData.duplicateId;
        } else if (errorType === 'SYSTEM_ERROR') {
            if (error?.code) errorItem.error_code = error.code;
            errorItem.retryable = itemData.retryable !== undefined ? itemData.retryable : true;
        } else if (errorType === 'INTEGRATION_ERROR') {
            if (itemData.integration) errorItem.integration = itemData.integration;
            if (error?.code) errorItem.error_code = error.code;
        }

        if (derivedName && !errorItem.name) errorItem.name = derivedName;
        if (itemData.email && !errorItem.email) errorItem.email = itemData.email;

        // Always expose contact name in contact/email scenarios when available
        if (!errorItem.contact_name && (errorType === 'VALIDATION_ERROR' || errorType === 'MISSING_DATA' || errorType === 'DUPLICATE_ERROR')) {
            if (derivedName) errorItem.contact_name = derivedName;
        }

        // Mark email as seen (first error wins)
        const finalEmailKey = (typeof errorItem.email === 'string' && errorItem.email.trim())
            ? errorItem.email.trim().toLowerCase()
            : null;
        if (finalEmailKey) this._seenEmails.add(finalEmailKey);

        this.errors[errorType].items.push(errorItem);
        this.errors[errorType].count++;
        this.updateSummary();
    }

    updateSummary() {
        // Summary fields are recalculated from scratch on every call to avoid negative or inflated counts
        // Reset all counters to prevent accumulation between calls
        this.errorSummary.total_errors = 0;
        this.errorSummary.retryable_errors = 0;
        this.errorSummary.critical_errors = 0;
        this.errorSummary.non_retryable_errors = 0;
        this.errorSummary.error_distribution = {};

        const retryableTypes = ['API_ERROR', 'SYSTEM_ERROR'];
        const criticalTypes = ['ENVIRONMENT_ERROR', 'QUEUE_MANAGEMENT_ERROR'];
        for (const [type, data] of Object.entries(this.errors)) {
            if (data.count > 0) {
                this.errorSummary.total_errors += data.count;
                this.errorSummary.error_distribution[type.toLowerCase()] = data.count;
                if (retryableTypes.includes(type)) this.errorSummary.retryable_errors += data.count;
                if (criticalTypes.includes(type)) this.errorSummary.critical_errors += data.count;
            }
        }
        this.errorSummary.non_retryable_errors = this.errorSummary.total_errors - this.errorSummary.retryable_errors;
    }

    buildSummary(errors) {
        const summary = {
            total_errors: 0,
            critical_errors: 0,
            retryable_errors: 0,
            non_retryable_errors: 0,
            error_distribution: {}
        };

        for (const [type, data] of Object.entries(errors)) {
            summary.total_errors += data.items.length;
            summary.error_distribution[type.toLowerCase()] = data.items.length;
        }

        summary.non_retryable_errors = summary.total_errors;
        return summary;
    }

    async saveErrors() {
        try {
            // Step 0: Validate we have new errors to save
            if (!this.errors || Object.keys(this.errors).length === 0) {
                console.log('[ERROR_MANAGER] No new errors — skipping save');
                return;
            }

            if (!this.objectType || !this.objectId) {
                console.error('[ERROR_MANAGER] Missing objectType or objectId');
                return;
            }

            console.log(`[ERROR_MANAGER] Processing ${Object.keys(this.errors).length} new error type(s)`);

            let existingErrors = {};

            // Step 1: Check if previous errors exist and decide merge strategy
            if (this.errorExists !== 'Yes' && this.errorExists !== true) {
                // FAST PATH: No previous errors - just convert new errors to proper structure
                console.log('[ERROR_MANAGER] No previous errors (error_exists != Yes) — saving new errors only');

                for (const [type, newData] of Object.entries(this.errors)) {
                    existingErrors[type] = {
                        count: newData.items?.length || 0,
                        items: newData.items || []
                    };
                }
            } else {
                // MERGE PATH: Previous errors exist - parse and merge with deduplication
                console.log('[ERROR_MANAGER] Previous errors exist — starting merge process');
                let raw = this.previousErrorLogs;

                // Step 2: Parse previous errors from input property
                if (raw) {
                    try {
                        let parsed = JSON.parse(raw);
                        // Handle double-stringified JSON
                        if (typeof parsed === 'string') {
                            parsed = JSON.parse(parsed);
                        }
                        // Extract errors: { errors: {...}, error_summary: {...} } format
                        existingErrors = parsed.errors || parsed || {};
                        console.log(`[ERROR_MANAGER] Loaded previous errors: ${Object.keys(existingErrors).length} type(s)`);
                    } catch (e) {
                        console.error('[ERROR_MANAGER] Failed to parse previous logs:', e.message);
                        existingErrors = {};
                    }
                } else {
                    console.log('[ERROR_MANAGER] Warning: error_exists=Yes but no previousErrorLogs found');
                    existingErrors = {};
                }

                // Step 3: Build email deduplication sets
                const existingEmailSet = new Set();
                const emailHasRootValidationError = new Set();

                let totalExistingErrors = 0;
                for (const [existingType, data] of Object.entries(existingErrors)) {
                    if (!data || !Array.isArray(data.items)) continue;

                    totalExistingErrors += data.items.length;

                    for (const item of data.items) {
                        const email = item.email?.trim()?.toLowerCase();
                        if (!email) continue;

                        existingEmailSet.add(email);

                        // Track root validation errors
                        if (existingType === 'VALIDATION_ERROR') {
                            const msg = (item.error || '').toLowerCase();
                            if (msg.includes('invalid email') || msg.includes('invalid format') ||
                                msg.includes('mx record') || msg.includes('no mx')) {
                                emailHasRootValidationError.add(email);
                            }
                        }
                    }
                }

                console.log(`[ERROR_MANAGER] Existing: ${totalExistingErrors} errors, ${existingEmailSet.size} unique emails, ${emailHasRootValidationError.size} root validation`);

                // Step 4: Merge new errors with deduplication
                let addedCount = 0;
                let skippedDuplicate = 0;
                let skippedDownstream = 0;

                for (const [type, newData] of Object.entries(this.errors)) {
                    if (!existingErrors[type]) {
                        existingErrors[type] = { count: 0, items: [] };
                    }

                    const incomingItems = Array.isArray(newData.items) ? newData.items : [];

                    for (const item of incomingItems) {
                        const email = item.email?.trim()?.toLowerCase();

                        // RULE 1: Skip if email already exists (cross-workflow deduplication)
                        if (email && existingEmailSet.has(email)) {
                            console.log(`[MERGE] Skip duplicate: ${email} (${type})`);
                            skippedDuplicate++;
                            continue;
                        }

                        // RULE 2: Skip downstream errors if root validation exists
                        if (email && emailHasRootValidationError.has(email) && type !== 'VALIDATION_ERROR') {
                            console.log(`[MERGE] Skip downstream: ${email} (${type})`);
                            skippedDownstream++;
                            continue;
                        }

                        // RULE 3: Add the error
                        existingErrors[type].items.push(item);
                        if (email) existingEmailSet.add(email);
                        addedCount++;
                    }

                    existingErrors[type].count = existingErrors[type].items.length;
                }

                console.log(`[ERROR_MANAGER] Merge result: +${addedCount} added, -${skippedDuplicate} duplicate, -${skippedDownstream} downstream`);
            }

            // Step 5: Clean up empty error types (remove types with 0 items)
            for (const [type, data] of Object.entries(existingErrors)) {
                if (!data.items || data.items.length === 0) {
                    delete existingErrors[type];
                    console.log(`[ERROR_MANAGER] Removed empty error type: ${type}`);
                }
            }

            // Step 6: Build final error data structure
            const errorData = {
                errors: existingErrors,
                error_summary: this.buildSummary(existingErrors)
            };

            // Step 7: Single API call to save to HubSpot
            await this.hubspotClient.apiRequest({
                method: 'PATCH',
                path: `/crm/v3/objects/${this.objectType}/${this.objectId}`,
                body: {
                    properties: {
                        connectspace_error_logs: JSON.stringify(errorData, null, 2),
                        error_exists: 'Yes'
                    }
                }
            });

            console.log(`[ERROR_MANAGER] ✓ Saved ${errorData.error_summary.total_errors} total errors`);

        } catch (error) {
            console.error('[ERROR_MANAGER] Failed to save errors:', error.message);
        }
    }
}

// Constants for association types
const ASSOCIATION_TYPES = {
    REGISTERED: 36,    // registered_for
    ATTENDEE: 32,      // attendees
    NO_SHOW: 34,       // no show
};

// Utility functions for consistent data handling
const DataUtils = {
    /**
     * Safely parse JSON string with error handling
     * @param {string} jsonString - The JSON string to parse
     * @param {object} defaultValue - Default value to return if parsing fails
     * @returns {object} - Parsed object or default value
     */
    safeParseJson(jsonString, defaultValue = {}) {
        try {
            return JSON.parse(jsonString);
        } catch (error) {
            console.error('Error parsing JSON:', error.message);
            return defaultValue;
        }
    },

    /**
     * Safely stringify object with error handling
     * @param {object} data - The object to stringify
     * @param {string} defaultValue - Default value to return if stringification fails
     * @returns {string} - Stringified object or default value
     */
    safeStringifyJson(data, defaultValue = '{}') {
        try {
            return JSON.stringify(data);
        } catch (error) {
            console.error('Error stringifying object:', error.message);
            return defaultValue;
        }
    },

    /**
     * Ensure value is a string
     * @param {any} value - The value to convert
     * @returns {string} - String representation of the value
     */
    ensureString(value) {
        if (value === null || value === undefined) return '';
        return String(value);
    },

    /**
     * Ensure all IDs in an array are strings
     * @param {Array} ids - Array of IDs
     * @returns {Array} - Array with all IDs as strings
     */
    ensureStringIds(ids) {
        if (!Array.isArray(ids)) return [];
        return ids.map(id => this.ensureString(id));
    }
};

/**
 * Maps ConnectSpace registration status to HubSpot association type ID
 */
function mapRegistrationStatus(status) {
    const statusMap = {
        'registered': ASSOCIATION_TYPES.REGISTERED,
        'attended': ASSOCIATION_TYPES.ATTENDEE,
        'no_show': ASSOCIATION_TYPES.NO_SHOW
    };
    return statusMap[status.toLowerCase()] || ASSOCIATION_TYPES.REGISTERED;
}

/**
 * Fetches all registrations for an event from ConnectSpace
 */
async function fetchAllRegistrations(client, eventId) {
    console.log(`[INFO] Fetching registrations for event ${eventId}`);

    let page = 1;
    let allRegistrations = [];
    let hasMore = true;

    while (hasMore) {
        try {
            const response = await client.get(
                `/events/${eventId}/registrations`,
                { params: { per_page: 100, page } }
            );

            const registrations = response.data.registrations || [];
            allRegistrations = allRegistrations.concat(registrations);
            hasMore = registrations.length >= 100;
            page++;
        } catch (error) {
            console.error(`[ERROR] Failed to fetch registrations page ${page}:`, error.message);
            hasMore = false;
        }
    }

    console.log(`[INFO] Total registrations fetched: ${allRegistrations.length}`);

    // Deduplicate by account_id (keep most recent)
    const uniqueRegistrations = new Map();
    allRegistrations.forEach(reg => {
        if (!reg.account_id) return;

        const previous = uniqueRegistrations.get(String(reg.account_id));
        if (!previous || new Date(reg.created_at) > new Date(previous.created_at)) {
            uniqueRegistrations.set(String(reg.account_id), reg);
        }
    });

    return Array.from(uniqueRegistrations.values());
}

/**
 * Finds HubSpot contact IDs for ConnectSpace account IDs
 */
// async function findHubSpotContactIds(client, accountIds) {
//     console.log(`[INFO] Finding HubSpot contacts for ${accountIds.length} account IDs`);

//     const contactMap = new Map();

//     // Process in batches of 100 (HubSpot's limit)
//     for (let i = 0; i < accountIds.length; i += 100) {
//         const batchIds = accountIds.slice(i, i + 100).map(id => String(id));
//       	console.log("batchid->",batchIds)
//         try {
//             const searchRequest = {
//                 filterGroups: [{
//                     filters: [{
//                         propertyName: 'cs_account_id',
//                         operator: 'IN',
//                         values: batchIds
//                     }]
//                 }],
//                 properties: ['cs_account_id', 'email', 'firstname', 'lastname'],
//                 limit: 100
//             };

//             const response = await client.crm.contacts.searchApi.doSearch(searchRequest);
//           console.log("Actual Response===>",response.results)
// 			console.log("[UJ]", "search response length ",response.results.length);
//           	console.log("[UJ]", "total search response length ",response.total);
//           const d = response.results.reduce((m,c)=>(m[c.properties?.cs_account_id]=(m[c.properties?.cs_account_id]||[]).concat(c),m),{});
// Object.entries(d).filter(([,v])=>v.length>1)
//   .forEach(([k,v])=>console.log('[DUP]', k, v.map(x=>({email:x.properties?.email, hs_object_id:x.id}))));

//             response.results.forEach(contact => {
//                 const accountId = contact.properties.cs_account_id;
//                 if (accountId) {
//                     contactMap.set(String(accountId), contact.id);
//                 } else {
// //                   console.log("[UJ]","Batch ids ",batchIds);	
//                 }
//             });
//         } catch (error) {
//             console.error('[ERROR] Contact search failed:', error.message);
//         }
//     }

//     console.log(`[INFO] Found ${contactMap.size} matching contacts in HubSpot`);
//     return contactMap;
// }
//batch is failing due to bad data in hus
async function findHubSpotContactIds(client, accountIds) {
    console.log(`[INFO] Finding HubSpot contacts for ${accountIds.length} account IDs`);

    // accountId -> Set(contactIds)
    const contactMap = new Map();

    for (let i = 0; i < accountIds.length; i += 100) {
        const batchIds = accountIds
            .slice(i, i + 100)
            .map(id => String(id).trim());

        console.log(`[INFO] Processing batch ${i / 100 + 1}, size=${batchIds.length}`);

        let after = undefined;
        let totalFetched = 0;

        try {
            do {
                const response = await client.crm.contacts.searchApi.doSearch({
                    filterGroups: [{
                        filters: [{
                            propertyName: 'cs_account_id',
                            operator: 'IN',
                            values: batchIds
                        }]
                    }],
                    properties: ['cs_account_id', 'email', 'firstname', 'lastname'],
                    limit: 100,
                    after,
                    archived: false
                });

                totalFetched += response.total;

                response.results.forEach(contact => {
                    const accountId = contact.properties?.cs_account_id?.trim();
                    if (!accountId) return;

                    if (!contactMap.has(accountId)) {
                        contactMap.set(accountId, new Set());
                    }
                    contactMap.get(accountId).add(contact.id);
                });

                after = response.paging?.next?.after;
            } while (after);

            console.log(`[INFO] Batch fetched ${totalFetched} contacts`);
        } catch (error) {
            console.error('[ERROR] Contact search failed:', error.message);
        }
    }

    console.log(`[INFO] Found ${contactMap.size} unique cs_account_id values in HubSpot`);
    return contactMap;
}

/**
 * Finds HubSpot company IDs for ConnectSpace company IDs
 */
async function findHubSpotCompanyIds(client, companyIds) {
    console.log(`[INFO] Finding HubSpot companies for ${companyIds.length} company IDs`);

    const companyMap = new Map();

    // Process in batches of 100 (HubSpot's limit)
    for (let i = 0; i < companyIds.length; i += 100) {
        const batchIds = companyIds.slice(i, i + 100).map(id => String(id));

        try {
            const searchRequest = {
                filterGroups: [{
                    filters: [{
                        propertyName: 'connectspace_company_id',
                        operator: 'IN',
                        values: batchIds
                    }]
                }],
                properties: ['connectspace_company_id'],
                limit: 100
            };

            const response = await client.crm.companies.searchApi.doSearch(searchRequest);

            response.results.forEach(company => {
                const companyId = company.properties.connectspace_company_id;
                if (companyId) {
                    companyMap.set(String(companyId), company.id);
                }
            });
        } catch (error) {
            console.error('[ERROR] Company search failed:', error.message);
        }
    }

    console.log(`[INFO] Found ${companyMap.size} matching companies in HubSpot`);
    return companyMap;
}

// Queue Manager Class
class QueueManager {
    constructor(processType, metadata = {}) {
        this.data = {
            processType,
            version: "1.0",
            timestamp: new Date().toISOString(),
            metadata: {
                totalCount: metadata.totalCount || 0,
                processedCount: metadata.processedCount || 0,
                failedCount: metadata.failedCount || 0,
                currentBatch: 0,  // Always start at 0 for new preparation
                retryCount: metadata.retryCount || 0,
                lastProcessedTimestamp: null,
                workflowControlId: metadata.workflowControlId || null,
                eventId: metadata.eventId,
                connectSpaceEventId: metadata.connectSpaceEventId,
                totalRegistrations: metadata.totalRegistrations
            },
            items: {
                remaining: [],
                processed: [],
                failed: []
            }
        };
    }

    setItems(items) {
        this.data.items.remaining = items;
        this.data.metadata.totalCount = items.length;
        return this;
    }

    getNextBatch() {
        return this.data.items.remaining.slice(0, this.data.metadata.batchSize);
    }

    markProcessed(items) {
        this.data.items.processed.push(...DataUtils.ensureStringIds(items));
        this.data.metadata.processedCount += items.length;
        this.data.metadata.lastProcessedTimestamp = new Date().toISOString();
        return this;
    }

    markFailed(items, errors) {
        const failedItems = DataUtils.ensureStringIds(items);
        this.data.items.failed.push(...failedItems);
        this.data.metadata.failedCount += items.length;
        return this;
    }

    updateRemaining() {
        this.data.items.remaining = this.data.items.remaining
            .slice(this.data.metadata.batchSize);
        this.data.metadata.currentBatch++;
        return this;
    }

    getWorkflowStatus() {
        // Always move to the next step (07_ASC_BATCH_A) regardless of errors
        // This ensures errors are passed to the next step without stopping the workflow
        if (this.data.items.remaining.length === 0) {
            console.log('[INFO] No associations to process, marking as complete');
            return '99_WFL_COMPLETE';
        }

        // Start with batch A for association processing
        console.log('[INFO] Moving to association batch processing');
        return '07_ASC_BATCH_A';
    }

    static fromProperty(property) {
        try {
            const data = DataUtils.safeParseJson(property);
            const queue = new QueueManager(data.processType);
            queue.data = data;

            // Ensure all IDs are strings for consistency
            if (queue.data.items) {
                if (Array.isArray(queue.data.items.processed)) {
                    queue.data.items.processed = DataUtils.ensureStringIds(queue.data.items.processed);
                }

                if (Array.isArray(queue.data.items.failed)) {
                    queue.data.items.failed = DataUtils.ensureStringIds(queue.data.items.failed);
                }
            }

            return queue;
        } catch (error) {
            console.error('Failed to parse queue data:', error);
            return null;
        }
    }

    toHubSpotProperties() {
        return {
            cs_processing_queue_status: this.getWorkflowStatus(),
            cs_processing_queue_data: DataUtils.safeStringifyJson(this.data)
        };
    }

    static getWorkflowStatuses() {
        return [
            '06_ASC_PREPARE',
            '07_ASC_BATCH_A',
            '07_ASC_BATCH_B',
            '99_WFL_COMPLETE',
            '99_WFL_ERROR'
        ];
    }
}

// Base Workflow Action Class
class WorkflowAction {
    constructor(event, callback) {
        this.event = event;
        this.callback = callback;
        this.hubspotClient = null;
        this.connectSpaceClient = null;
        this.queue = null;
        this.errorManager = null;
    }

    async initialize() {
        // Validate environment variables
        const requiredVars = [
            'HUBSPOT_ACCESS_TOKEN',
            'CONNECT_SPACE_API_KEY',
            'CONNECT_SPACE_BASE_URL',
            'HUBSPOT_EVENT_OBJECT_ID'
        ];

        const missing = requiredVars.filter(varName => !process.env[varName]);
        if (missing.length > 0) {
            throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
        }

        // Initialize API clients
        this.hubspotClient = new hubspot.Client({
            accessToken: process.env.HUBSPOT_ACCESS_TOKEN
        });

        this.connectSpaceClient = axios.create({
            baseURL: process.env.CONNECT_SPACE_BASE_URL.replace(/\/+$/, '') + '/api/v1',
            headers: {
                'X-Team-Api-Auth-Token': process.env.CONNECT_SPACE_API_KEY,
                'Content-Type': 'application/json'
            },
            timeout: 9000
        });

        // Initialize error manager with previous error logs from input properties
        const objectId = this.event.inputFields.hs_object_id || this.event.objectId;
        const previousErrorLogs = this.event.inputFields.connectspace_error_logs || null;
        const errorExists = this.event.inputFields.error_exists || null;

        this.errorManager = new ErrorManager(
            this.hubspotClient,
            process.env.HUBSPOT_EVENT_OBJECT_ID,
            objectId,
            previousErrorLogs,
            errorExists
        );
        await this.errorManager.loadErrors();
    }

    async updateHubSpotEventProperties(eventId, outputFields) {
        try {
            const properties = {
                cs_processing_queue_data: outputFields.cs_processing_queue_data
            };

            console.log(`Updating event ${eventId} with properties:`);
            console.log('- cs_processing_queue_data length:', properties.cs_processing_queue_data?.length || 0);

            await this.hubspotClient.crm.objects.basicApi.update(
                process.env.HUBSPOT_EVENT_OBJECT_ID,
                eventId,
                { properties }
            );

            console.log('✓ Event properties updated successfully in HubSpot');
        } catch (error) {
            console.error('✗ Error updating HubSpot event properties:', error.message);
            if (error.body) {
                console.error('Error details:', JSON.stringify(error.body, null, 2));
            }
        }
    }

    async run() {
        try {
            await this.initialize();
            await this.execute();
        } catch (error) {
            console.error('Workflow action failed:', error);
            if (this.errorManager) {
                this.errorManager.logError(error, {
                    eventId: this.event.inputFields.hs_object_id || this.event.objectId
                });
                await this.errorManager.saveErrors();
            }
            this.callback({
                outputFields: {
                    cs_processing_queue_status: '99_WFL_ERROR',
                    cs_processing_queue_data: DataUtils.safeStringifyJson({
                        error: `Workflow failed: ${WORKFLOW_NAME} - ${error.message}`,
                        workflow: WORKFLOW_NAME,
                        timestamp: new Date().toISOString()
                    })
                }
            });
        }
    }
}

// Event Association Action Implementation
class PrepareEventAssociationsAction extends WorkflowAction {
    async execute() {
        const eventId = this.event.inputFields.hs_object_id || this.event.objectId;
        const connectSpaceEventId = this.event.inputFields.cs_event_id;

        if (!eventId || !connectSpaceEventId) {
            throw new Error('Missing required event IDs');
        }

        console.log(`[START] Preparing associations for event ID: ${eventId} (CS: ${connectSpaceEventId})`);

        // Preserve previous errors without failing the workflow
        // NOTE: As of the latest update, previous steps (3a.js, 3b.js) now track ACCOUNT IDs directly
        // No conversion needed - we receive account IDs and pass them forward
        let failedAccountIds = new Set(); // Track account IDs for failed contacts (from 3a.js/3b.js)
        let previousFailedCount = 0;

        // OLD: Used to track registration IDs and convert them
        // let failedRegistrationIds = new Set();
        // let failedAccountIds = new Set();

        if (this.event.inputFields.cs_processing_queue_data) {
            try {
                const previousQueueData = DataUtils.safeParseJson(this.event.inputFields.cs_processing_queue_data);

                // Get failed ACCOUNT IDs from previous step (3a.js or 3b.js)
                if (previousQueueData.items && Array.isArray(previousQueueData.items.failed)) {
                    previousQueueData.items.failed.forEach(id => {
                        const failedId = typeof id === 'object' && id.item ? id.item : id;
                        failedAccountIds.add(String(failedId));
                    });

                    console.log(`[INFO] Found ${failedAccountIds.size} previously failed account IDs`);
                }

                // Preserve failed count
                if (previousQueueData.metadata && previousQueueData.metadata.failedCount) {
                    previousFailedCount = previousQueueData.metadata.failedCount;
                }
            } catch (error) {
                console.error('[ERROR] Failed to parse previous queue data:', error);
                if (this.errorManager) {
                    this.errorManager.logError(error, {
                        eventId: eventId
                    }, {
                        queueRelated: true
                    });
                }
            }
        }

        // Fetch all registrations from ConnectSpace
        let registrations = [];
        try {
            registrations = await fetchAllRegistrations(this.connectSpaceClient, connectSpaceEventId);
        } catch (error) {
            console.error('[ERROR] Failed to fetch registrations:', error);
            if (this.errorManager) {
                this.errorManager.logError(error, {
                    eventId: eventId
                }, {
                    apiEndpoint: `/events/${connectSpaceEventId}/registrations`,
                    integration: 'connectspace'
                });
            }
            throw error;
        }

        // Get unique account IDs from ALL registration
        const allAccountIds = [...new Set(registrations.filter(reg => reg.account_id).map(reg => String(reg.account_id)))];
      console.log("[UJ]","ALLACCOUNTIDS ",allAccountIds.length);
        // Check if these account IDs exist in HubSpot
        const contactMap = await findHubSpotContactIds(this.hubspotClient, allAccountIds);
        // Find account IDs not found in HubSpot
        const notFoundAccountIds = allAccountIds.filter(
            id => !contactMap.has(String(id).trim())
        );
        console.log("[UJ]"," Not account ids: ",notFoundAccountIds)
        if (notFoundAccountIds.length > 0) {
            console.log(`[WARN] ${notFoundAccountIds.length} account IDs not found in HubSpot`);

            // Log missing contact errors and add account IDs to failed set
            if (this.errorManager) {
                registrations.forEach(reg => {
                    if (reg.account_id && notFoundAccountIds.includes(String(reg.account_id))) {
                        this.errorManager.logError(new Error('Contact not found in HubSpot'), {
                            registrationId: reg.id,
                            accountId: reg.account_id,
                            email: reg.email,
                            name: getDisplayName(reg)
                        }, {
                            field: 'account_id',
                            integration: 'hubspot'
                        });
                        // Add ACCOUNT ID to failed set (not registration ID)
                        failedAccountIds.add(String(reg.account_id));
                    }
                });
            } else {
                // Mark account IDs with missing contacts as failures
                notFoundAccountIds.forEach(accountId => {
                    failedAccountIds.add(String(accountId));
                });
            }
        }

        // Check for end date and determine if event has ended
        let eventHasEnded = false;

        if (this.event.inputFields.end_date) {
            try {
                let eventEndDate;
                const endDateValue = this.event.inputFields.end_date;

                if (/^\d+$/.test(endDateValue)) {
                    eventEndDate = new Date(parseInt(endDateValue, 10));
                } else {
                    eventEndDate = new Date(endDateValue);
                }

                const currentDate = new Date();

                if (isNaN(eventEndDate.getTime())) {
                    throw new Error("Invalid date object created");
                }

                eventHasEnded = eventEndDate < currentDate;
                console.log(`[INFO] Event end date: ${eventEndDate.toISOString()}`);
                console.log(`[INFO] Event has ${eventHasEnded ? 'ended' : 'not ended yet'}`);

            } catch (error) {
                console.log(`[WARN] Issue parsing end_date: "${this.event.inputFields.end_date}". Assuming event has not ended.`);
                eventHasEnded = false;
            }
        } else {
            console.log('[INFO] No end_date provided. Assuming event has not ended.');
            eventHasEnded = false;
        }

        // Group registrations by status
        const registered = [];
        const attended = [];
        const noShow = [];
        const contactToCompany = [];
        const skippedDueToFailure = [];
        const skippedDueToNoContact = [];

        // Process registrations
        registrations.forEach(reg => {
            if (!reg.account_id) return;

            const accountIdStr = String(reg.account_id);
            const regIdStr = String(reg.id);

            // Skip accounts that failed in previous steps (now checking ACCOUNT IDs)
            if (failedAccountIds.has(accountIdStr)) {
                skippedDueToFailure.push(accountIdStr);
                return;
            }

            // Skip registrations where contact doesn't exist in HubSpot
            if (!contactMap.has(accountIdStr)) {
                skippedDueToNoContact.push(accountIdStr);
                failedAccountIds.add(accountIdStr); // Add ACCOUNT ID to failed set
                return;
            }

            // Store contact-company relationship if both exist
            if (reg.account_id && reg.company_id) {
                contactToCompany.push({
                    contactId: accountIdStr,
                    companyId: String(reg.company_id)
                });
            }

            // Categorize by status
            if (reg.attended) {
                attended.push(accountIdStr);
            } else if (eventHasEnded && !reg.canceled_at) {
                noShow.push(accountIdStr);
            } else {
                registered.push(accountIdStr);
            }
        });

        // Log summary of skipped registrations
        if (skippedDueToFailure.length > 0 || skippedDueToNoContact.length > 0) {
            console.log(`[INFO] Skipped: ${skippedDueToFailure.length} due to previous failures, ${skippedDueToNoContact.length} due to missing contacts`);
        }

        // Calculate total contacts to process
        const totalContacts = registered.length + attended.length + noShow.length;
        console.log(`[INFO] Processing: ${registered.length} registered, ${attended.length} attended, ${noShow.length} no-show`);

        // Calculate total failed count (now using account IDs)
        const totalFailedCount = failedAccountIds.size;

        // Initialize queue with correct metadata
        this.queue = new QueueManager('ASSOCIATION_PROCESSING', {
            eventId: DataUtils.ensureString(eventId),
            connectSpaceEventId: DataUtils.ensureString(connectSpaceEventId),
            totalRegistrations: registrations.length,
            workflowControlId: this.event.objectId ? DataUtils.ensureString(this.event.objectId) : null,
            totalCount: totalContacts,
            failedCount: totalFailedCount
        });

        // Structure the remaining items in the format 07 expects
        const queueItems = [{
            eventId: DataUtils.ensureString(eventId),
            connectSpaceEventId: DataUtils.ensureString(connectSpaceEventId),
            registered: DataUtils.ensureStringIds(registered) || [],
            attended: DataUtils.ensureStringIds(attended) || [],
            noShow: DataUtils.ensureStringIds(noShow) || [],
            contactToCompany: contactToCompany || []
        }];

        // Set the items in the queue
        this.queue.setItems(queueItems);

        // CRITICAL: Pass ACCOUNT IDs in the failed array (not registration IDs)
        // This ensures 7a.js receives the same ID type as in registered/attended/noShow arrays
        this.queue.data.items.failed = DataUtils.ensureStringIds(Array.from(failedAccountIds));

        console.log(`[SUCCESS] Association preparation completed: ${totalContacts} contacts to process, ${failedAccountIds.size} failed account IDs`);
        console.log('Queue status:', this.queue.getWorkflowStatus());

        // Save errors before sending response
        if (this.errorManager) {
            await this.errorManager.saveErrors();
        }

        const outputFields = this.queue.toHubSpotProperties();

        // Optionally update HubSpot directly via API
        if (process.env.UPDATE_HUBSPOT_DIRECTLY === 'true') {
            console.log('\n=== Updating HubSpot Event via API ===');
            await this.updateHubSpotEventProperties(eventId, outputFields);
        }

        // Send response with the standardized queue structure
        this.callback({
            outputFields: outputFields
        });
    }
}

exports.main = async (event, callback) => {
    console.log('Beginning association preparation process');
    const action = new PrepareEventAssociationsAction(event, callback);
    await action.run();
};