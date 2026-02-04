/**
 * HubSpot Workflow Action: Process Registration Batch
 * -----------------------------------------------
 * For Workflow: [Events] | 03[A/B] Update Event | Process Registrations
 * 
 * Description:
 * Creates or updates contact records in HubSpot based on event registration data.
 * Part of A/B batch processing loop for handling large volumes of registrations.
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
 * - cs_processing_queue_status: String (Current batch status A/B)
 * 
 * Output Properties:
 * - cs_processing_queue_status: String (Next batch status or completion)
 * - cs_processing_queue_data: String (Updated queue data)
 * 
 */

// Required HubSpot packages
const hubspot = require('@hubspot/api-client');
const axios = require('axios');
const dns = require('dns').promises;

// Workflow identification constant
const WORKFLOW_NAME = "03A_REG_BATCH";

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
        if (msg.includes('invalid email') || msg.includes('invalid format') || msg.includes('mx record') || msg.includes('mx records')
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
            // OLD: Prioritized registrationId over accountId
            // id: itemData.id || itemData.eventId || itemData.registrationId || 'unknown',

            // NEW: Prioritize accountId for contact-related errors (more consistent with failed array)
            id: itemData.id || itemData.eventId || itemData.accountId || itemData.registrationId || 'unknown',
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

// Configuration Constants
const CONFIG = {
    BATCH_SIZE: 25,   // Number of registrations to process in each batch
    RETRY_LIMIT: 3,   // Maximum number of retries for failed operations
    EMAIL_VALIDATION: {
        CHECK_MX_RECORDS: true  // Set to true to enable MX record validation (slower but more accurate)
    }
};

// Utility Classes
class APIClientFactory {
    static createHubSpotClient(accessToken) {
        return new hubspot.Client({ accessToken });
    }

    static createConnectSpaceClient(apiKey, baseURL) {
        return axios.create({
            baseURL: baseURL.replace(/\/+$/, '') + '/api/v1',
            headers: {
                'X-Team-Api-Auth-Token': apiKey,
                'Content-Type': 'application/json'
            },
        });
    }
}

class EnvironmentValidator {
    static validate(requiredVars) {
        const missing = requiredVars.filter(varName => !process.env[varName]);
        if (missing.length > 0) {
            throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
        }
    }
}

// Queue Manager Class
class QueueManager {
    constructor(processType, metadata = {}) {
        this.data = {
            processType,
            version: '1.0',
            timestamp: new Date().toISOString(),
            metadata: {
                totalCount: 0,
                processedCount: 0,
                failedCount: 0,
                batchSize: CONFIG.BATCH_SIZE,
                currentBatch: 0,
                retryCount: 0,
                lastProcessedTimestamp: null,
                failToError: false,
                ...metadata
            },
            items: {
                remaining: [],
                processed: [],
                failed: []
            }
        };
    }
    /**
     * Set items to be processed
     * Note: For registration processing, these are ConnectSpace registration IDs (not account IDs)
     * The registration IDs are used for tracking in the queue, but account IDs are used for contact matching
     */
    setItems(items) {
        // Ensure all items are strings for consistent comparison
        this.data.items.remaining = items.map(item => String(item));
        this.data.metadata.totalCount = items.length;
        return this;
    }
    /**
     * Process next batch of items
     * Note: For registration processing, these are ConnectSpace registration IDs (not account IDs)
     */
    getNextBatch() {
        return this.data.items.remaining.slice(0, CONFIG.BATCH_SIZE);
    }
    /**
     * Mark items as processed
     * Note: For registration processing, these are ConnectSpace registration IDs (not account IDs)
     */
    markProcessed(items) {
        // Ensure all items are strings for consistent comparison
        const stringItems = items.map(item => String(item));
        this.data.items.processed.push(...stringItems);
        this.data.metadata.processedCount += stringItems.length;
        this.data.metadata.lastProcessedTimestamp = new Date().toISOString();

        // Simple log of success count
        console.log(`[SUCCESS] ${stringItems.length} registrations processed`);

        return this;
    }
    /**
     * Mark items as failed
     * Note: For registration processing, these are ConnectSpace registration IDs (not account IDs)
     */
    markFailed(items, errors) {
        // Ensure all items are strings for consistent comparison
        const stringItems = items.map(item => String(item));

        // Add the string IDs directly to the failed array
        this.data.items.failed.push(...stringItems);

        // Simple log of failure count
        console.log(`[FAIL] ${stringItems.length} registrations failed`);

        this.data.metadata.failedCount += stringItems.length;
        return this;
    }
    /**
     * Update remaining items
     * Note: For registration processing, these are ConnectSpace registration IDs (not account IDs)
     */
    updateRemaining() {
        this.data.items.remaining = this.data.items.remaining
            .slice(CONFIG.BATCH_SIZE);
        this.data.metadata.currentBatch++;
        return this;
    }
    /**
     * Get the current workflow status
     */
    getWorkflowStatus(currentStatus) {
        // Check if all items have been processed
        if (!this.data.items.remaining || this.data.items.remaining.length === 0) {
            console.log('[INFO] All items processed, moving to next step');
            return '06_ASC_PREPARE'; // Move to next step when complete
        }

        // Continue the A/B loop
        const nextStatus = currentStatus === '03_REG_BATCH_A' ? '03_REG_BATCH_B' : '03_REG_BATCH_A';
        console.log(`[INFO] Continuing batch processing with status: ${nextStatus}`);
        return nextStatus;
    }
    /**
     * Convert queue to HubSpot properties
     */
    toHubSpotProperties(currentStatus) {
        return {
            cs_processing_queue_status: this.getWorkflowStatus(currentStatus),
            cs_processing_queue_data: JSON.stringify(this.data)
        };
    }
    /**
     * Create queue from property
     */
    static fromProperty(property) {
        try {
            // Handle both JSON string and object
            let data;
            if (typeof property === 'string') {
                data = JSON.parse(property);
            } else if (typeof property === 'object' && property !== null) {
                // Already an object, use it directly
                data = property;
            } else {
                console.error('Invalid property type:', typeof property);
                return null;
            }

            // Validate that we have the minimum required data structure
            if (!data || !data.processType) {
                console.error('Invalid queue data: missing processType');
                return null;
            }

            const queue = new QueueManager(data.processType);

            // Merge the loaded data with the initialized data to ensure all required fields exist
            queue.data = {
                ...queue.data, // Start with initialized default structure
                ...data,       // Override with loaded data
                metadata: {
                    ...queue.data.metadata, // Start with default metadata
                    ...(data.metadata || {}) // Override with loaded metadata if exists
                },
                items: {
                    ...queue.data.items, // Start with default items
                    ...(data.items || {}) // Override with loaded items if exists
                }
            };

            return queue;
        } catch (error) {
            console.error('Failed to parse queue data:', error);
            return null;
        }
    }

    static getWorkflowStatuses() {
        return [
            '01_EVT_BATCH_A',
            '01_EVT_BATCH_B',
            '01C_EVT_SCHEDULE',
            '02_REG_FETCH',
            '03_REG_BATCH_A',
            '03_REG_BATCH_B',
            '04_CMP_FETCH',
            '05_CMP_BATCH_A',
            '05_CMP_BATCH_B',
            '06_ASC_PREPARE',
            '07_ASC_BATCH_A',
            '07_ASC_BATCH_B',
            '99_WFL_COMPLETE',
            '99_WFL_ERROR'
        ];
    }

    getFailedIds() {
        if (!this.data.items.failed || !Array.isArray(this.data.items.failed)) {
            return [];
        }

        // Handle both formats: simple IDs and objects with item property
        return this.data.items.failed.map(item => {
            if (typeof item === 'object' && item.item) {
                return String(item.item);
            }
            return String(item);
        });
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

    async run() {
        try {
            await this.initialize();
            await this.execute();
        } catch (error) {
            console.error('Workflow action failed:', error);
            if (this.errorManager) {
                this.errorManager.logError(error, {
                    eventId: this.getEventId()
                });
                await this.errorManager.saveErrors();
            }
            this.sendErrorResponse(error);
        }
    }

    async initialize() {
        // Validate required environment variables
        EnvironmentValidator.validate([
            'HUBSPOT_ACCESS_TOKEN',
            'CONNECT_SPACE_API_KEY',
            'CONNECT_SPACE_BASE_URL',
            'HUBSPOT_EVENT_OBJECT_ID'
        ]);

        // Initialize API clients
        this.hubspotClient = APIClientFactory.createHubSpotClient(
            process.env.HUBSPOT_ACCESS_TOKEN
        );

        this.connectSpaceClient = APIClientFactory.createConnectSpaceClient(
            process.env.CONNECT_SPACE_API_KEY,
            process.env.CONNECT_SPACE_BASE_URL
        );

        // Initialize error manager with previous error logs from input properties
        const objectId = this.getEventId();
        const objectType = process.env.HUBSPOT_EVENT_OBJECT_ID;
        const previousErrorLogs = this.event.inputFields.connectspace_error_logs || null;
        const errorExists = this.event.inputFields.error_exists || null;

        if (!objectId) {
            console.warn('[ERROR_MANAGER] No object ID found - errorManager will not be initialized');
        } else if (!objectType) {
            console.warn('[ERROR_MANAGER] HUBSPOT_EVENT_OBJECT_ID secret not found - errorManager will not be initialized');
        } else {
            this.errorManager = new ErrorManager(
                this.hubspotClient,
                objectType,
                objectId,
                previousErrorLogs,
                errorExists
            );
            await this.errorManager.loadErrors();
            console.log(`[ERROR_MANAGER] Initialized for object ${objectId} (type: ${objectType})`);
        }
    }

    async execute() {
        const eventId = this.getEventId();
        const connectSpaceEventId = this.getConnectSpaceEventId();

        console.log('[START] Processing registration batch');

        if (!connectSpaceEventId) {
            throw new Error('Missing ConnectSpace event ID');
        }

        this.loadQueue();
        if (!this.queue) {
            throw new Error('Failed to load queue data');
        }

        // Get registration IDs from the queue
        const registrationIds = this.queue.getNextBatch();
        if (registrationIds.length === 0) {
            console.log('[INFO] No registrations to process');
            await this.sendQueueResponse();
            return;
        }

        // Fetch registrations from ConnectSpace
        let registrations = [];
        try {
            registrations = await this.fetchRegistrationsForBatch(connectSpaceEventId, registrationIds);
        } catch (error) {
            if (this.errorManager) {
                this.errorManager.logError(error, {
                    eventId: this.getEventId()
                }, {
                    apiEndpoint: `/events/${connectSpaceEventId}/registrations`,
                    integration: 'connectspace'
                });
            }
            throw error;
        }
        console.log(`[INFO] Processing ${registrations.length} registrations`);

        // Filter out registrations without account IDs
        const validRegistrations = registrations.filter(reg => reg.account_id);
        const skippedRegistrations = registrations.filter(reg => !reg.account_id);

        // Log missing account_id errors
        if (skippedRegistrations.length > 0 && this.errorManager) {
            skippedRegistrations.forEach(reg => {
                // OLD: Used registrationId only
                // this.errorManager.logError(new Error('Missing required field: account_id'), {
                //     registrationId: reg.id,
                //     email: reg.email,
                //     name: getDisplayName(reg)
                // }, {
                //     field: 'account_id'
                // });

                // NEW: Include both registrationId and accountId (accountId will be null/undefined here)
                this.errorManager.logError(new Error('Missing required field: account_id'), {
                    registrationId: reg.id,  // Keep for reference since account_id is missing
                    accountId: reg.account_id || null,  // Will be null, but explicit
                    email: reg.email,
                    name: getDisplayName(reg)
                }, {
                    field: 'account_id'
                });
            });
        }

        try {
            // Process valid registrations in one batch
            const { processedResults, skippedRegistrationIds } = await this.upsertContacts(validRegistrations);

            // Get IDs of successfully processed registrations
            const successfulIds = validRegistrations
                .filter(reg => !skippedRegistrationIds.includes(reg.id))
                .map(reg => reg.id);

            // Get IDs of all skipped registrations
            const allSkippedIds = [
                ...skippedRegistrations.map(reg => reg.id),
                ...skippedRegistrationIds
            ];

            // Mark successful registrations as processed
            if (successfulIds.length > 0) {
                this.queue.markProcessed(successfulIds);
            }

            // Add skipped registrations directly to failed array
            if (allSkippedIds.length > 0) {
                console.log(`[SKIP] ${allSkippedIds.length} registrations (no account ID or invalid email)`);

                // Ensure errors are logged for all skipped IDs
                if (this.errorManager) {
                    const loggedErrorIds = new Set();
                    // Collect all logged error IDs
                    for (const [type, data] of Object.entries(this.errorManager.errors)) {
                        if (data.items) {
                            data.items.forEach(item => {
                                if (item.id) loggedErrorIds.add(String(item.id));
                                if (item.registrationId) loggedErrorIds.add(String(item.registrationId));
                            });
                        }
                    }

                    // Log errors for any skipped IDs that weren't already logged
                    allSkippedIds.forEach(id => {
                        const idStr = String(id);
                        if (!loggedErrorIds.has(idStr)) {
                            console.log(`[WARN] Registration ${idStr} was skipped but no error was logged - logging generic error`);

                            // OLD: Used registrationId for tracking
                            // this.errorManager.logError(new Error('Registration skipped during processing'), {
                            //     registrationId: idStr,
                            //     eventId: this.getEventId()
                            // }, {
                            //     field: 'processing'
                            // });

                            // NEW: Try to find account_id from registration (if available)
                            const registration = registrations.find(reg => String(reg.id) === idStr);
                            this.errorManager.logError(new Error('Registration skipped during processing'), {
                                accountId: registration?.account_id || null,  // Use account_id if available
                                registrationId: idStr,                         // Keep for reference
                                eventId: this.getEventId()
                            }, {
                                field: 'processing'
                            });
                        }
                    });
                }

                this.queue.data.items.failed.push(...allSkippedIds.map(id => String(id)));
                this.queue.data.metadata.failedCount += allSkippedIds.length;
            }

            // Update remaining items in the queue
            this.queue.updateRemaining();

            // Final summary
            console.log(`[SUMMARY] Total: ${registrationIds.length}, Success: ${successfulIds.length}, Failed/Skipped: ${allSkippedIds.length}`);

        } catch (error) {
            console.error('[ERROR] Batch processing failed:', error.message);
            if (this.errorManager) {
                this.errorManager.logError(error, {
                    eventId: this.getEventId()
                }, {
                    apiEndpoint: '/crm/v3/objects/contacts/batch',
                    integration: 'hubspot'
                });
            }
            // Add registration IDs directly to failed array
            this.queue.data.items.failed.push(...registrationIds.map(id => String(id)));
            this.queue.data.metadata.failedCount += registrationIds.length;
            this.queue.updateRemaining();
        }

        await this.sendQueueResponse();
    }

    async fetchRegistrationsForBatch(eventId, registrationIds) {
        try {
            console.log(`[API:ConnectSpace] Fetching registrations for batch of ${registrationIds.length}`);

            // Convert all IDs to strings for consistent comparison
            const registrationIdStrings = registrationIds.map(id => String(id));

            // Fetch registrations in batches to avoid large responses
            let page = 1;
            let allRegistrations = [];
            let hasMore = true;
            let foundCount = 0;

            while (hasMore && foundCount < registrationIdStrings.length) {
                const response = await this.connectSpaceClient.get(
                    `/events/${eventId}/registrations`,
                    { params: { per_page: 100, page } }
                );

                const registrations = response.data.registrations || [];

                // Filter to only include registrations in our batch
                const batchRegistrations = registrations.filter(reg =>
                    registrationIdStrings.includes(String(reg.id))
                );

                allRegistrations = allRegistrations.concat(batchRegistrations);
                foundCount = allRegistrations.length;

                // Stop if we've found all registrations or no more results
                hasMore = registrations.length >= 100 && foundCount < registrationIdStrings.length;
                page++;
            }

            // Deduplicate by account_id (keep most recent registration)
            const uniqueRegistrations = new Map();

            allRegistrations.forEach(reg => {
                if (!reg.account_id) return;

                const accountIdStr = String(reg.account_id);
                const previous = uniqueRegistrations.get(accountIdStr);

                if (!previous || new Date(reg.created_at) > new Date(previous.created_at)) {
                    uniqueRegistrations.set(accountIdStr, reg);
                }
            });

            return Array.from(uniqueRegistrations.values());
        } catch (error) {
            console.error(`[ERROR] Fetch registrations failed: ${error.message}`);
            if (this.errorManager) {
                this.errorManager.logError(error, {
                    eventId: this.getEventId()
                }, {
                    apiEndpoint: `/events/${eventId}/registrations`,
                    integration: 'connectspace'
                });
            }
            throw error;
        }
    }

    async upsertContacts(registrations) {
        try {
            console.log(`[INFO] Processing ${registrations.length} contacts`);

            // Get only essential properties to check
            let propertyNames = ['email', 'firstname', 'lastname', 'cs_account_id', 'jobtitle', 'phone', 'company', 'hs_additional_emails'];
            try {
                const propertiesResponse = await this.hubspotClient.crm.properties.coreApi.getAll('contacts');
                propertyNames = propertiesResponse.results.map(prop => prop.name);
            } catch (schemaError) {
                console.log('[WARN] Using core properties only');
            }

            // Define property types for data handling
            const PROPERTY_TYPES = {
                STRING: 'string',
                NUMBER: 'number',
                BOOLEAN: 'boolean',
                DATE: 'date',
                DATETIME: 'datetime'
            };

            // Define property mappings
            const propertyMappings = [
                // Core Contact Properties
                { hubspotProp: 'email', connectSpaceProp: 'email', type: PROPERTY_TYPES.STRING },
                { hubspotProp: 'firstname', connectSpaceProp: 'first_name', type: PROPERTY_TYPES.STRING },
                { hubspotProp: 'lastname', connectSpaceProp: 'last_name', type: PROPERTY_TYPES.STRING },
                { hubspotProp: 'cs_account_id', connectSpaceProp: 'account_id', type: PROPERTY_TYPES.STRING },

                // Professional Info
                { hubspotProp: 'jobtitle', connectSpaceProp: 'title', type: PROPERTY_TYPES.STRING },
                { hubspotProp: 'phone', connectSpaceProp: 'phone', type: PROPERTY_TYPES.STRING },
                { hubspotProp: 'company', connectSpaceProp: 'company', type: PROPERTY_TYPES.STRING },

                // Registration Details
                { hubspotProp: 'cs_registration_id', connectSpaceProp: 'id', type: PROPERTY_TYPES.STRING },
                { hubspotProp: 'cs_registration_created_at', connectSpaceProp: 'created_at', type: PROPERTY_TYPES.DATETIME },
                { hubspotProp: 'cs_registration_updated_at', connectSpaceProp: 'updated_at', type: PROPERTY_TYPES.DATETIME },
                { hubspotProp: 'cs_event_id', connectSpaceProp: 'event_id', type: PROPERTY_TYPES.STRING },
                { hubspotProp: 'cs_full_name', connectSpaceProp: 'full_name', type: PROPERTY_TYPES.STRING },
                { hubspotProp: 'cs_attended', connectSpaceProp: 'attended', type: PROPERTY_TYPES.BOOLEAN },
                { hubspotProp: 'cs_checked_in_at', connectSpaceProp: 'checked_in_at', type: PROPERTY_TYPES.DATETIME },
                { hubspotProp: 'cs_confirmation_number', connectSpaceProp: 'confirmation_number', type: PROPERTY_TYPES.STRING },
                { hubspotProp: 'cs_registrant_type_id', connectSpaceProp: 'registrant_type_id', type: PROPERTY_TYPES.STRING },
                { hubspotProp: 'cs_canceled_at', connectSpaceProp: 'canceled_at', type: PROPERTY_TYPES.DATETIME },
                { hubspotProp: 'cs_submitted_at', connectSpaceProp: 'submitted_at', type: PROPERTY_TYPES.DATETIME },
            ];

            // Filter mappings to only include properties that exist in HubSpot
            const validMappings = propertyMappings.filter(mapping =>
                propertyNames.includes(mapping.hubspotProp)
            );

            // Map registrations to properties
            const validRegistrationsWithProperties = [];
            const skippedRegistrationIds = [];

            // Process and validate registrations in one pass
            for (const registration of registrations) {
                const properties = {};
                const email = registration.email ? registration.email.toLowerCase() : null;

                // Email validation
                let isValidEmail = false;
                if (email) {
                    const validationResult = await EmailValidator.validate(email, CONFIG.EMAIL_VALIDATION.CHECK_MX_RECORDS);
                    isValidEmail = validationResult.valid;

                    if (!isValidEmail) {
                        console.log(`[WARN] Invalid email for registration ${registration.id}: ${validationResult.reason}`);
                        if (this.errorManager) {
                            // OLD: Used registrationId for tracking
                            // this.errorManager.logError(new Error(validationResult.reason || 'Invalid email'), {
                            //     registrationId: registration.id,
                            //     email: email,
                            //     name: getDisplayName(registration)
                            // }, {
                            //     field: 'email'
                            // });

                            // NEW: Use accountId for contact-based errors (more consistent with failed array)
                            this.errorManager.logError(new Error(validationResult.reason || 'Invalid email'), {
                                accountId: registration.account_id,  // Primary ID for contact tracking
                                // registrationId: registration.id,     // Keep for reference
                                email: email,
                                name: getDisplayName(registration)
                            }, {
                                field: 'email'
                            });
                        }
                    }
                }

                // Skip if no email or invalid email
                if (!email || !isValidEmail) {
                    skippedRegistrationIds.push(registration.id);
                    continue;
                }

                // Map properties based on filtered mappings
                for (const mapping of validMappings) {
                    const value = registration[mapping.connectSpaceProp];
                    if (value !== null && value !== undefined) {
                        // Format the value based on its type
                        if (mapping.type === PROPERTY_TYPES.DATETIME && value) {
                            // Format ISO date string for HubSpot
                            try {
                                // Properly handle ConnectSpace date format with timezone
                                // Format: "2025-02-18 18:38:37 +0000"
                                const date = new Date(value);
                                // Check if date is valid
                                if (isNaN(date.getTime())) {
                                    console.log(`[WARN] Invalid date for ${mapping.hubspotProp}: ${value}, using string value`);
                                    properties[mapping.hubspotProp] = String(value);
                                } else {
                                    // Valid date - convert to ISO string for HubSpot
                                    properties[mapping.hubspotProp] = date.toISOString();
                                }
                            } catch (error) {
                                console.log(`[ERROR] Date conversion failed for ${mapping.hubspotProp}: ${error.message}`);
                                properties[mapping.hubspotProp] = String(value);
                            }
                        } else if (mapping.type === PROPERTY_TYPES.DATE && value) {
                            // Format as YYYY-MM-DD for HubSpot date properties
                            try {
                                const date = new Date(value);
                                if (isNaN(date.getTime())) {
                                    console.log(`[WARN] Invalid date for ${mapping.hubspotProp}: ${value}, using string value`);
                                    properties[mapping.hubspotProp] = String(value);
                                } else {
                                    // Extract just the date portion as YYYY-MM-DD
                                    properties[mapping.hubspotProp] = date.toISOString().split('T')[0];
                                }
                            } catch (error) {
                                console.log(`[ERROR] Date conversion failed for ${mapping.hubspotProp}: ${error.message}`);
                                properties[mapping.hubspotProp] = String(value);
                            }
                        } else if (mapping.type === PROPERTY_TYPES.BOOLEAN) {
                            // Convert to "true"/"false" strings for HubSpot
                            properties[mapping.hubspotProp] = value ? "true" : "false";
                        } else if (mapping.transform) {
                            // Apply custom transformation if specified
                            properties[mapping.hubspotProp] = mapping.transform(value);
                        } else {
                            // Default to string conversion
                            properties[mapping.hubspotProp] = String(value);
                        }
                    }
                }

                validRegistrationsWithProperties.push({
                    registration,
                    properties,
                    email,
                    connectSpaceId: registration.account_id ? String(registration.account_id) : null
                });
            }

            console.log(`[INFO] Found ${validRegistrationsWithProperties.length} valid registrations, ${skippedRegistrationIds.length} skipped`);

            // Deduplicate by email (keep first occurrence)
            const emailMap = new Map();
            const uniqueRegistrations = [];

            validRegistrationsWithProperties.forEach(reg => {
                if (!emailMap.has(reg.email)) {
                    emailMap.set(reg.email, true);
                    uniqueRegistrations.push(reg);
                }
            });

            // Create maps to track existing contacts and processed registrations
            let existingContactsByEmail = {};
            let processedRegistrations = new Set();
            let remainingRegistrations = [...uniqueRegistrations];

            // STEP 1: Check for existing contacts by primary email
            const allEmails = uniqueRegistrations.map(reg => reg.email);

            if (allEmails.length > 0) {
                console.log(`[INFO] STEP 1: Searching for ${allEmails.length} contacts by primary email`);

                try {
                    const MAX_BATCH_SIZE = 100;

                    for (let i = 0; i < allEmails.length; i += MAX_BATCH_SIZE) {
                        const emailBatch = allEmails.slice(i, i + MAX_BATCH_SIZE);

                        try {
                            // Create batch search request for primary emails
                            const searchRequest = {
                                filterGroups: [{
                                    filters: [{
                                        propertyName: 'email',
                                        operator: 'IN',
                                        values: emailBatch
                                    }]
                                }],
                                properties: ['email', 'hs_additional_emails', 'cs_account_id'],
                                limit: 100
                            };

                            const searchResponse = await this.hubspotClient.crm.contacts.searchApi.doSearch(searchRequest);

                            if (searchResponse && searchResponse.results) {
                                searchResponse.results.forEach(contact => {
                                    const email = contact.properties.email?.toLowerCase();
                                    if (email) {
                                        existingContactsByEmail[email] = {
                                            id: contact.id,
                                            properties: contact.properties
                                        };
                                    }
                                });
                            }
                        } catch (searchError) {
                            console.error(`[ERROR] Primary email search failed: ${searchError.message}`);
                        }
                    }

                    // Mark registrations found by primary email as processed
                    remainingRegistrations = uniqueRegistrations.filter(reg => {
                        const found = !!existingContactsByEmail[reg.email];
                        if (found) {
                            processedRegistrations.add(reg.email);
                        }
                        return !found;
                    });

                    console.log(`[INFO] Found ${processedRegistrations.size} contacts by primary email, ${remainingRegistrations.length} remaining`);
                } catch (searchError) {
                    console.error(`[ERROR] Email search failed: ${searchError.message}`);
                }
            }

            // STEP 2: For remaining registrations, check secondary emails
            if (remainingRegistrations.length > 0) {
                console.log(`[INFO] STEP 2: Searching for ${remainingRegistrations.length} contacts by secondary email`);

                const emailsToCheck = remainingRegistrations.map(reg => reg.email);
                let foundBySecondaryEmail = 0;

                // We need to search one by one for secondary emails
                for (const reg of remainingRegistrations) {
                    if (processedRegistrations.has(reg.email)) continue;

                    try {
                        // Create search request for secondary emails using CONTAINS operator
                        const searchRequest = {
                            filterGroups: [{
                                filters: [{
                                    propertyName: 'hs_additional_emails',
                                    operator: 'CONTAINS_TOKEN',
                                    value: reg.email
                                }]
                            }],
                            properties: ['email', 'hs_additional_emails', 'cs_account_id'],
                            limit: 10
                        };

                        const searchResponse = await this.hubspotClient.crm.contacts.searchApi.doSearch(searchRequest);

                        if (searchResponse && searchResponse.results && searchResponse.results.length > 0) {
                            // Verify the email is actually in the additional emails list
                            for (const contact of searchResponse.results) {
                                const additionalEmails = contact.properties.hs_additional_emails || '';
                                const emailList = additionalEmails.split(';').map(e => e.trim().toLowerCase());

                                if (emailList.includes(reg.email)) {
                                    existingContactsByEmail[reg.email] = {
                                        id: contact.id,
                                        properties: contact.properties
                                    };
                                    processedRegistrations.add(reg.email);
                                    foundBySecondaryEmail++;
                                    break; // Found a match, no need to check other contacts
                                }
                            }
                        }
                    } catch (searchError) {
                        console.error(`[ERROR] Secondary email search failed for ${reg.email}: ${searchError.message}`);
                    }
                }

                // Update remaining registrations
                remainingRegistrations = remainingRegistrations.filter(reg => !processedRegistrations.has(reg.email));
                console.log(`[INFO] Found ${foundBySecondaryEmail} contacts by secondary email, ${remainingRegistrations.length} remaining`);
            }

            // STEP 3: For remaining registrations, check by ConnectSpace ID
            let existingContactsByConnectSpaceId = {};

            if (remainingRegistrations.length > 0) {
                const connectSpaceIdsToCheck = remainingRegistrations
                    .map(reg => reg.connectSpaceId)
                    .filter(Boolean); // Filter out null/undefined values

                if (connectSpaceIdsToCheck.length > 0) {
                    console.log(`[INFO] STEP 3: Searching for ${connectSpaceIdsToCheck.length} contacts by ConnectSpace ID`);

                    try {
                        const MAX_BATCH_SIZE = 100;

                        for (let i = 0; i < connectSpaceIdsToCheck.length; i += MAX_BATCH_SIZE) {
                            const idBatch = connectSpaceIdsToCheck.slice(i, i + MAX_BATCH_SIZE);

                            try {
                                // Create batch search request for ConnectSpace IDs
                                const searchRequest = {
                                    filterGroups: [{
                                        filters: [{
                                            propertyName: 'cs_account_id',
                                            operator: 'IN',
                                            values: idBatch.map(id => String(id))
                                        }]
                                    }],
                                    properties: ['email', 'hs_additional_emails', 'cs_account_id'],
                                    limit: 100
                                };

                                const searchResponse = await this.hubspotClient.crm.contacts.searchApi.doSearch(searchRequest);

                                if (searchResponse && searchResponse.results) {
                                    searchResponse.results.forEach(contact => {
                                        const csId = contact.properties.cs_account_id;
                                        if (csId) {
                                            existingContactsByConnectSpaceId[csId] = {
                                                id: contact.id,
                                                properties: contact.properties
                                            };
                                        }
                                    });
                                }
                            } catch (searchError) {
                                console.error(`[ERROR] ConnectSpace ID search failed: ${searchError.message}`);
                            }
                        }

                        console.log(`[INFO] Found ${Object.keys(existingContactsByConnectSpaceId).length} contacts by ConnectSpace ID`);
                    } catch (searchError) {
                        console.error(`[ERROR] ConnectSpace ID search failed: ${searchError.message}`);
                    }
                }
            }

            // Categorize contacts for processing
            const contactsToCreate = [];
            const contactsToUpdate = [];
            const secondaryEmailsToAdd = [];

            uniqueRegistrations.forEach(reg => {
                // Case 1: Contact exists by primary or secondary email - simple update
                if (existingContactsByEmail[reg.email]) {
                    contactsToUpdate.push({
                        id: existingContactsByEmail[reg.email].id,
                        properties: reg.properties
                    });
                }
                // Case 2: Contact exists by ConnectSpace ID but with different email
                else if (reg.connectSpaceId && existingContactsByConnectSpaceId[reg.connectSpaceId]) {
                    const existingContact = existingContactsByConnectSpaceId[reg.connectSpaceId];
                    const existingEmail = existingContact.properties.email?.toLowerCase();

                    // Don't try to update hs_additional_emails directly as it's read-only
                    // Just update the contact with other properties
                    contactsToUpdate.push({
                        id: existingContact.id,
                        properties: reg.properties
                    });

                    // If the registration email is different from the primary email,
                    // we'll add it as a secondary email in a separate API call after the update
                    if (existingEmail !== reg.email) {
                        // Track this for later processing
                        secondaryEmailsToAdd.push({
                            contactId: existingContact.id,
                            email: reg.email
                        });
                    }
                }
                // Case 3: No existing contact found - create new
                else {
                    contactsToCreate.push({
                        properties: reg.properties
                    });
                }
            });

            // Process contacts in batches
            const results = [];
            const MAX_BATCH_SIZE = 100;

            // Create new contacts
            if (contactsToCreate.length > 0) {
                console.log(`[API:HubSpot] Creating ${contactsToCreate.length} new contacts`);

                for (let i = 0; i < contactsToCreate.length; i += MAX_BATCH_SIZE) {
                    const batch = contactsToCreate.slice(i, i + MAX_BATCH_SIZE);

                    try {
                        const createResponse = await this.hubspotClient.apiRequest({
                            method: 'POST',
                            path: '/crm/v3/objects/contacts/batch/create',
                            body: { inputs: batch }
                        });

                        if (createResponse && createResponse.results) {
                            results.push(...createResponse.results.map(contact => ({
                                contactId: contact.id,
                                status: 'created'
                            })));
                        }
                    } catch (error) {
                        // Handle CONFLICT errors
                        if (error.response?.body?.errors) {
                            const conflicts = error.response.body.errors.filter(e =>
                                e.category === 'CONFLICT' &&
                                e.message?.includes('Contact already exists') &&
                                e.context?.index !== undefined
                            );

                            for (const conflict of conflicts) {
                                const idMatch = conflict.message.match(/Existing ID: (\d+)/);
                                if (idMatch && idMatch[1]) {
                                    const existingId = idMatch[1];
                                    const contactIndex = conflict.context.index;

                                    if (contactIndex < batch.length) {
                                        contactsToUpdate.push({
                                            id: existingId,
                                            properties: batch[contactIndex].properties
                                        });
                                    }
                                }
                            }

                            if (conflicts.length > 0) {
                                console.log(`[INFO] Moved ${conflicts.length} conflicts to update`);
                            }
                        } else {
                            console.error(`[ERROR] Create failed: ${error.message}`);
                            if (this.errorManager) {
                                batch.forEach(contact => {
                                    this.errorManager.logError(error, {
                                        email: contact.properties.email,
                                        name: `${contact.properties.firstname || ''} ${contact.properties.lastname || ''}`.trim()
                                    }, {
                                        apiEndpoint: '/crm/v3/objects/contacts/batch/create',
                                        integration: 'hubspot'
                                    });
                                });
                            }
                        }
                    }
                }
            }

            // Update existing contacts
            if (contactsToUpdate.length > 0) {
                console.log(`[API:HubSpot] Updating ${contactsToUpdate.length} existing contacts`);

                for (let i = 0; i < contactsToUpdate.length; i += MAX_BATCH_SIZE) {
                    const batch = contactsToUpdate.slice(i, i + MAX_BATCH_SIZE);

                    try {
                        const updateResponse = await this.hubspotClient.apiRequest({
                            method: 'POST',
                            path: '/crm/v3/objects/contacts/batch/update',
                            body: { inputs: batch }
                        });

                        if (updateResponse && updateResponse.results) {
                            results.push(...updateResponse.results.map(contact => ({
                                contactId: contact.id,
                                status: 'updated'
                            })));
                        }
                    } catch (error) {
                        console.error(`[ERROR] Update failed: ${error.message}`);
                        if (this.errorManager) {
                            batch.forEach(contact => {
                                this.errorManager.logError(error, {
                                    eventId: contact.id,
                                    email: contact.properties?.email
                                }, {
                                    apiEndpoint: '/crm/v3/objects/contacts/batch/update',
                                    integration: 'hubspot'
                                });
                            });
                        }
                    }
                }
            }

            // Process secondary emails
            if (secondaryEmailsToAdd.length > 0) {
                console.log(`[INFO] Adding ${secondaryEmailsToAdd.length} secondary emails`);

                for (const item of secondaryEmailsToAdd) {
                    try {
                        // Use the proper endpoint to add a secondary email
                        await this.hubspotClient.apiRequest({
                            method: 'PUT',
                            path: `/contacts/v1/contact/email/${encodeURIComponent(item.email)}/profile`,
                            body: {
                                vid: item.contactId
                            }
                        });

                        console.log(`[INFO] Added ${item.email} as secondary email for contact ${item.contactId}`);
                    } catch (error) {
                        // Check if this is a duplicate email error
                        if (error.response?.body?.status === 'error' &&
                            error.response?.body?.message?.includes('Contact already exists')) {
                            console.log(`[WARN] Email ${item.email} already exists as a primary email for another contact`);
                        } else {
                            console.error(`[ERROR] Failed to add secondary email ${item.email}: ${error.message}`);
                            if (this.errorManager) {
                                this.errorManager.logError(error, {
                                    email: item.email,
                                    eventId: item.contactId
                                }, {
                                    apiEndpoint: '/contacts/v1/contact/email/profile',
                                    integration: 'hubspot'
                                });
                            }
                        }
                    }
                }
            }

            // Final summary log
            const createdCount = results.filter(r => r.status === 'created').length;
            const updatedCount = results.filter(r => r.status === 'updated').length;
            console.log(`[SUMMARY] Created: ${createdCount}, Updated: ${updatedCount}`);

            return {
                processedResults: results,
                skippedRegistrationIds: skippedRegistrationIds
            };
        } catch (error) {
            console.error(`[ERROR] Contact processing failed: ${error.message}`);
            if (this.errorManager) {
                this.errorManager.logError(error, {
                    eventId: this.getEventId()
                }, {
                    apiEndpoint: '/crm/v3/objects/contacts/batch/upsert',
                    integration: 'hubspot'
                });
            }
            throw error;
        }
    }

    async updateEvent(eventId, properties) {
        try {
            console.log(`[API:HubSpot] Updating event: ${eventId}`);
            const response = await this.hubspotClient.apiRequest({
                method: 'PATCH',
                path: `/crm/v3/objects/${process.env.HUBSPOT_EVENT_OBJECT_ID}/${eventId}`,
                body: { properties }
            });
            return response;
        } catch (error) {
            console.error(`[API:HubSpot] Failed to update event:`, error);
            // Don't throw here, as this is just for debugging
        }
    }

    async batchReadByProperty(values, propertyName) {
        if (!values || values.length === 0) {
            return {};
        }

        console.log(`[INFO] Batch searching ${values.length} contacts by ${propertyName}`);
        // Add validation for property name
        if (propertyName === 'cs_account_id') {
            console.log(`[INFO] Using ConnectSpace account_id for contact lookup (not registration_id)`);
        }

        const results = {};

        // Maximum batch size for batch operations
        const MAX_BATCH_SIZE = 100;

        // Process in batches
        for (let i = 0; i < values.length; i += MAX_BATCH_SIZE) {
            const batch = values.slice(i, i + MAX_BATCH_SIZE);
            console.log(`[INFO] Processing batch ${Math.floor(i / MAX_BATCH_SIZE) + 1} of ${Math.ceil(values.length / MAX_BATCH_SIZE)}`);

            try {
                // Use search API instead of batch read
                const searchRequest = {
                    filterGroups: [{
                        filters: [{
                            propertyName: propertyName,
                            operator: 'IN',
                            values: batch.map(value => String(value))
                        }]
                    }],
                    properties: ['email', 'firstname', 'lastname', 'cs_account_id'],
                    limit: 100
                };

                const response = await this.hubspotClient.crm.contacts.searchApi.doSearch(searchRequest);

                if (response && response.results) {
                    response.results.forEach(contact => {
                        const key = propertyName === 'email'
                            ? contact.properties.email.toLowerCase()
                            : contact.properties[propertyName];

                        if (key) {
                            results[key] = {
                                id: contact.id,
                                hasConnectSpaceId: !!contact.properties.cs_account_id,
                                properties: contact.properties
                            };
                        }
                    });
                }
            } catch (error) {
                console.error(`[ERROR] Batch search failed for ${propertyName}:`, error.message);
                // Continue with next batch instead of failing completely
            }
        }

        return results;
    }
}

// Event Action Base Class
class EventActionBase extends WorkflowAction {
    initializeQueue(processType, metadata = {}) {
        this.queue = new QueueManager(processType, metadata);
    }

    loadQueue() {
        if (this.event.inputFields.cs_processing_queue_data) {
            try {
                this.queue = QueueManager.fromProperty(
                    this.event.inputFields.cs_processing_queue_data
                );
                console.log('Queue data loaded successfully');
            } catch (error) {
                console.error('Failed to parse queue data:', error);
                if (this.errorManager) {
                    this.errorManager.logError(error, {
                        eventId: this.getEventId()
                    }, {
                        queueRelated: true
                    });
                }
                return null;
            }
        }
    }

    async sendQueueResponse() {
        if (!this.queue) {
            throw new Error('Queue not initialized');
        }

        // Save errors before sending response
        if (this.errorManager) {
            console.log(`[ERROR_MANAGER] Total errors before save: ${this.errorManager.errorSummary.total_errors}`);
            console.log(`[ERROR_MANAGER] Error types: ${Object.keys(this.errorManager.errors).filter(k => this.errorManager.errors[k].count > 0).join(', ')}`);
            await this.errorManager.saveErrors();
        } else {
            console.log('[ERROR_MANAGER] ErrorManager is null - errors will not be saved');
        }

        // Get the workflow status from the queue
        const status = this.queue.getWorkflowStatus(this.event.inputFields.cs_processing_queue_status);
        console.log(`[INFO] Returning workflow status: ${status}`);

        const outputFields = this.queue.toHubSpotProperties(this.event.inputFields.cs_processing_queue_status);

        // Optionally update HubSpot directly via API
        if (process.env.UPDATE_HUBSPOT_DIRECTLY === 'true') {
            const eventId = this.getEventId();
            console.log('\n=== Updating HubSpot Event via API ===');
            await this.updateHubSpotEventProperties(eventId, outputFields);
        }

        // Use the standard queue properties without overriding
        this.callback({
            outputFields: outputFields
        });
    }

    async updateHubSpotEventProperties(eventId, outputFields) {
        try {
            const properties = {
                cs_processing_queue_status: outputFields.cs_processing_queue_status,
                cs_processing_queue_data: outputFields.cs_processing_queue_data
            };

            console.log(`Updating event ${eventId} with properties:`);
            console.log('- cs_processing_queue_status:', properties.cs_processing_queue_status);
            console.log('- cs_processing_queue_data length:', properties.cs_processing_queue_data?.length || 0);

            await this.hubspotClient.crm.objects.basicApi.update(
                process.env.HUBSPOT_EVENT_OBJECT_ID,
                eventId,
                { properties }
            );

            console.log('✓ Event properties updated successfully in HubSpot');
        } catch (error) {
            console.error('✗ Error updating HubSpot event properties:', error.message);

            // Log detailed error information for debugging dropdown issues
            if (error.body) {
                console.error('Error details:', JSON.stringify(error.body, null, 2));
            }

            // Don't throw - we still want to return the response to the workflow
            // The workflow will update the properties anyway
        }
    }

    async processBatch(processor) {
        const batch = this.queue.getNextBatch();
        console.log(`[INFO] Processing batch of ${batch.length} items`);

        const results = [];
        const errors = [];

        for (const item of batch) {
            try {
                console.log(`[INFO] Processing item: ${item}`);
                const result = await processor(item);
                results.push(result);
                errors.push(null);
            } catch (error) {
                console.error(`Failed to process item:`, error);
                results.push(null);
                errors.push(error);
            }
        }

        const successfulItems = batch.filter((_, index) => !errors[index]);
        const failedItems = batch.filter((_, index) => errors[index]);

        this.queue
            .markProcessed(successfulItems)
            .markFailed(failedItems, errors.filter(Boolean))
            .updateRemaining();

        return {
            successful: successfulItems,
            failed: failedItems,
            results: results.filter(Boolean)
        };
    }

    getEventId() {
        return this.event.inputFields.hs_object_id ||
            this.event.objectId;
    }

    getConnectSpaceEventId() {
        return this.event.inputFields.cs_event_id;
    }

    sendErrorResponse(error) {
        this.callback({
            outputFields: {
                cs_processing_queue_status: '99_WFL_ERROR',
                cs_processing_queue_data: JSON.stringify({
                    error: `Workflow failed: ${WORKFLOW_NAME} - ${error.message}`,
                    workflow: WORKFLOW_NAME,
                    timestamp: new Date().toISOString()
                })
            }
        });
    }
}

// Email validation utility class
class EmailValidator {
    /**
     * Validates email format using regex
     * @param {string} email - The email address to validate
     * @returns {boolean} - True if the email format is valid, false otherwise
     */
    static isValidFormat(email) {
        if (!email || typeof email !== 'string') return false;
        const emailRegex = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
        return emailRegex.test(email);
    }

    /**
     * Checks if the email domain has valid MX records
     * @param {string} email - The email address to validate
     * @returns {Promise<boolean>} - True if the domain has MX records, false otherwise
     */
    static async hasMXRecord(email) {
        try {
            if (!email || typeof email !== 'string') return false;
            if (!this.isValidFormat(email)) return false;

            const domain = email.split('@')[1];
            const records = await dns.resolveMx(domain);
            return records && records.length > 0;
        } catch (error) {
            console.log(`[WARN] MX lookup failed for ${email}: ${error.message}`);
            return false;
        }
    }

    /**
     * Validates an email address
     * @param {string} email - The email address to validate
     * @param {boolean} checkMX - Whether to check MX records
     * @returns {Promise<{valid: boolean, reason: string}>} - Validation result
     */
    static async validate(email, checkMX = false) {
        if (!email || typeof email !== 'string') {
            return { valid: false, reason: "Empty or invalid email" };
        }

        if (!this.isValidFormat(email)) {
            return { valid: false, reason: "Invalid email format" };
        }

        if (checkMX) {
            const hasMX = await this.hasMXRecord(email);
            if (!hasMX) {
                return { valid: false, reason: "Domain has no MX records" };
            }
        }

        return { valid: true, reason: "Valid email" };
    }
}

// Action Implementation
class ProcessRegistrationBatchAction extends EventActionBase {
    async execute() {
        const connectSpaceEventId = this.getConnectSpaceEventId();

        console.log('[START] Processing registration batch');

        if (!connectSpaceEventId) {
            throw new Error('Missing ConnectSpace event ID');
        }

        this.loadQueue();
        if (!this.queue) {
            throw new Error('Failed to load queue data');
        }

        // Get registration IDs from the queue
        const registrationIds = this.queue.getNextBatch();
        if (registrationIds.length === 0) {
            console.log('[INFO] No registrations to process');
            await this.sendQueueResponse();
            return;
        }

        try {
            // Fetch registrations from ConnectSpace
            let registrations = [];
            try {
                registrations = await this.fetchRegistrationsForBatch(connectSpaceEventId, registrationIds);
            } catch (error) {
                if (this.errorManager) {
                    this.errorManager.logError(error, {
                        eventId: this.getEventId()
                    }, {
                        apiEndpoint: `/events/${connectSpaceEventId}/registrations`,
                        integration: 'connectspace'
                    });
                }
                throw error;
            }
            console.log(`[INFO] Processing ${registrations.length} registrations`);

            // Filter out registrations without account IDs
            const validRegistrations = registrations.filter(reg => reg.account_id);
            const skippedRegistrations = registrations.filter(reg => !reg.account_id);

            // Log missing account_id errors
            if (skippedRegistrations.length > 0 && this.errorManager) {
                skippedRegistrations.forEach(reg => {
                    // OLD: Used registrationId only
                    // this.errorManager.logError(new Error('Missing required field: account_id'), {
                    //     registrationId: reg.id,
                    //     email: reg.email,
                    //     name: getDisplayName(reg)
                    // }, {
                    //     field: 'account_id'
                    // });

                    // NEW: Include both registrationId and accountId (accountId will be null/undefined here)
                    this.errorManager.logError(new Error('Missing required field: account_id'), {
                        registrationId: reg.id,  // Keep for reference since account_id is missing
                        accountId: reg.account_id || null,  // Will be null, but explicit
                        email: reg.email,
                        name: getDisplayName(reg)
                    }, {
                        field: 'account_id'
                    });
                });
            }

            const skippedRegistrationIdsFromMissing = skippedRegistrations.map(reg => reg.id);

            // NEW: Extract account IDs from skipped registrations for failed array
            const skippedAccountIdsFromMissing = skippedRegistrations
                .filter(reg => reg.account_id)
                .map(reg => String(reg.account_id));

            // Process valid registrations
            const { processedResults, skippedRegistrationIds } = await this.upsertContacts(validRegistrations);

            // Get IDs of successfully processed registrations
            const successfulIds = validRegistrations
                .filter(reg => !skippedRegistrationIds.includes(reg.id))
                .map(reg => reg.id);

            // OLD: Get IDs of all skipped registrations (REGISTRATION IDs)
            // const allSkippedIds = [...skippedRegistrationIdsFromMissing, ...skippedRegistrationIds];

            // NEW: Get ACCOUNT IDs of all skipped registrations
            const skippedAccountIdsFromProcessing = validRegistrations
                .filter(reg => skippedRegistrationIds.includes(reg.id) && reg.account_id)
                .map(reg => String(reg.account_id));

            const allSkippedAccountIds = [...skippedAccountIdsFromMissing, ...skippedAccountIdsFromProcessing];

            console.log(`[DEBUG] Skipped: ${skippedAccountIdsFromMissing.length} from missing account_id, ${skippedAccountIdsFromProcessing.length} from processing`);

            // Update queue
            if (successfulIds.length > 0) {
                this.queue.markProcessed(successfulIds);
            }

            // OLD: Push registration IDs to failed array
            // if (allSkippedIds.length > 0) {
            //     console.log(`[SKIP] ${allSkippedIds.length} registrations skipped`);
            //     this.queue.data.items.failed.push(...allSkippedIds.map(id => String(id)));
            //     this.queue.data.metadata.failedCount += allSkippedIds.length;
            // }

            // NEW: Push ACCOUNT IDs to failed array
            if (allSkippedAccountIds.length > 0) {
                console.log(`[SKIP] ${allSkippedAccountIds.length} account IDs marked as failed`);
                this.queue.data.items.failed.push(...allSkippedAccountIds);
                this.queue.data.metadata.failedCount += allSkippedAccountIds.length;
            }

            this.queue.updateRemaining();
            console.log(`[SUMMARY] Total: ${registrationIds.length}, Success: ${successfulIds.length}, Failed: ${allSkippedAccountIds.length} account IDs`);

        } catch (error) {
            console.error(`[ERROR] Batch processing failed: ${error.message}`);
            if (this.errorManager) {
                this.errorManager.logError(error, {
                    eventId: this.getEventId()
                }, {
                    apiEndpoint: '/crm/v3/objects/contacts/batch',
                    integration: 'hubspot'
                });
            }

            // OLD: Push registration IDs to failed array
            // this.queue.data.items.failed.push(...registrationIds.map(id => String(id)));

            // NEW: Push ACCOUNT IDs to failed array (extract from registrations if available)
            if (registrations && registrations.length > 0) {
                const failedAccountIds = registrations
                    .filter(reg => reg.account_id)
                    .map(reg => String(reg.account_id));
                this.queue.data.items.failed.push(...failedAccountIds);
                this.queue.data.metadata.failedCount += failedAccountIds.length;
                console.log(`[ERROR] Marked ${failedAccountIds.length} account IDs as failed due to batch error`);
            } else {
                // Fallback: if we don't have registrations, we can't get account IDs
                console.log(`[ERROR] Cannot extract account IDs - registrations not available`);
                this.queue.data.metadata.failedCount += registrationIds.length;
            }

            this.queue.updateRemaining();
        }

        await this.sendQueueResponse();
    }

    async findContacts(accountIds) {
        console.log(`[INFO] Finding ${accountIds.length} contacts by account ID`);

        const contactMap = {};
        const notFoundIds = [];

        // Process in batches
        for (let i = 0; i < accountIds.length; i += 100) {
            const batchIds = accountIds.slice(i, i + 100);

            try {
                // Search for contacts by account ID
                const searchRequest = {
                    filterGroups: [{
                        filters: [{
                            propertyName: 'cs_account_id',
                            operator: 'IN',
                            values: batchIds.map(id => String(id))
                        }]
                    }],
                    properties: ['email', 'firstname', 'lastname', 'cs_account_id'],
                    limit: 100
                };

                const response = await this.hubspotClient.crm.contacts.searchApi.doSearch(searchRequest);

                // Process found contacts
                if (response && response.results) {
                    response.results.forEach(contact => {
                        const accountId = contact.properties.cs_account_id;
                        if (accountId) {
                            contactMap[accountId] = {
                                id: contact.id,
                                properties: contact.properties
                            };
                        }
                    });
                }

                // Simplified logging
                console.log(`[INFO] Found ${response.results.length} of ${batchIds.length} contacts in batch ${Math.floor(i / 100) + 1}`);

            } catch (error) {
                console.error('[ERROR] Contact search failed:', error.message);
            }
        }

        // Identify account IDs that weren't found
        notFoundIds.push(...accountIds.filter(id => !contactMap[id]));

        console.log(`[SUMMARY] Found ${Object.keys(contactMap).length} of ${accountIds.length} contacts`);
        return { contactMap, notFoundIds };
    }
}

module.exports.main = async (event, callback) => {
    const action = new ProcessRegistrationBatchAction(event, callback);
    await action.run();
};