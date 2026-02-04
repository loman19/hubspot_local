/**
 * HubSpot Workflow Action: Process Association Batch
 * -----------------------------------------------
 * For Workflow: [Events] | 07[A/B] Create Associations | Create Association Records
 * 
 * Description:
 * Processes batches of associations between contacts/companies and events.
 * Creates the actual association records in the HubSpot CRM.
 * Part of A/B batch processing loop for handling large volumes of associations.
 * 
 * Required Secrets:
 * - HUBSPOT_ACCESS_TOKEN: HubSpot Private App Access Token
 * - HUBSPOT_EVENT_OBJECT_ID: HubSpot Custom Object ID for Events
 * 
 * Input Properties:
 * - hs_object_id: String (HubSpot Event ID)
 * - cs_processing_queue_data: String (Queue data from previous step)
 * - cs_processing_queue_status: String (Current batch status A/B)
 * 
 * Output Properties:
 * - cs_processing_queue_status: String (Next batch status or completion)
 * - cs_processing_queue_data: String (Updated queue data)
 */

const hubspot = require('@hubspot/api-client');

// Workflow identification constant
const WORKFLOW_NAME = "07B_ASC_BATCH";

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
            // OLD: Prioritized contactId without accountId
            // id: itemData.id || itemData.eventId || itemData.contactId || 'unknown',

            // NEW: Prioritize accountId for contact-related errors (more consistent with failed array)
            id: itemData.id || itemData.accountId || itemData.eventId || itemData.contactId || 'unknown',
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
    BATCH_SIZE: 50,  // Number of associations to process in each batch
};

// Association type definitions
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

// Queue Manager Class
class QueueManager {
    constructor(data = {}) {
        this.data = data;

        // Initialize default structure if needed
        if (!this.data.metadata) {
            this.data.metadata = {
                totalCount: 0,
                processedCount: 0,
                failedCount: 0,
                currentBatch: 0,
                lastProcessedTimestamp: null
            };
        }

        if (!this.data.items) {
            this.data.items = {
                remaining: [],
                processed: [],
                failed: []
            };
        }

        // Ensure all IDs in arrays are strings for consistency
        this.ensureConsistentDataTypes();
    }

    ensureConsistentDataTypes() {
        // Ensure all IDs in arrays are strings
        if (this.data.items) {
            if (Array.isArray(this.data.items.processed)) {
                this.data.items.processed = DataUtils.ensureStringIds(this.data.items.processed);
            }

            if (Array.isArray(this.data.items.failed)) {
                this.data.items.failed = DataUtils.ensureStringIds(this.data.items.failed);
            }

            // Handle remaining items which might be complex objects
            if (Array.isArray(this.data.items.remaining)) {
                this.data.items.remaining.forEach(item => {
                    if (item) {
                        if (item.eventId) item.eventId = DataUtils.ensureString(item.eventId);

                        if (Array.isArray(item.registered)) {
                            item.registered = DataUtils.ensureStringIds(item.registered);
                        }

                        if (Array.isArray(item.attended)) {
                            item.attended = DataUtils.ensureStringIds(item.attended);
                        }

                        if (Array.isArray(item.noShow)) {
                            item.noShow = DataUtils.ensureStringIds(item.noShow);
                        }
                    }
                });
            }
        }
    }

    getWorkflowStatus(currentStatus) {
        // Count the actual number of accounts remaining
        let totalAccountsRemaining = 0;
        if (this.data.items.remaining && this.data.items.remaining.length > 0) {
            this.data.items.remaining.forEach(item => {
                const registered = Array.isArray(item.registered) ? item.registered.length : 0;
                const attended = Array.isArray(item.attended) ? item.attended.length : 0;
                const noShow = Array.isArray(item.noShow) ? item.noShow.length : 0;
                totalAccountsRemaining += registered + attended + noShow;
            });
        }

        console.log('Getting workflow status:');
        console.log(`- Remaining accounts to process: ${totalAccountsRemaining}`);
        console.log(`- Failed accounts: ${this.data.items.failed.length}`);

        // If no items remaining, determine final status
        if (this.data.items.remaining.length === 0) {
            if (this.data.items.failed && this.data.items.failed.length > 0) {
                return '99_WFL_ERROR';
            } else {
                return '99_WFL_COMPLETE';
            }
        }

        // Alternate between A and B
        return currentStatus === '07_ASC_BATCH_A' ? '07_ASC_BATCH_B' : '07_ASC_BATCH_A';
    }

    toHubSpotProperties(currentStatus) {
        return {
            cs_processing_queue_status: this.getWorkflowStatus(currentStatus),
            cs_processing_queue_data: DataUtils.safeStringifyJson(this.data)
        };
    }

    static fromProperty(property) {
        const data = DataUtils.safeParseJson(property);
        return new QueueManager(data);
    }
}

class AssociationProcessor {
    constructor(event, callback) {
        this.event = event;
        this.callback = callback;
        this.queue = null;
        this.hubspotClient = new hubspot.Client({
            accessToken: process.env.HUBSPOT_ACCESS_TOKEN
        });
        this.errors = []; // Track errors during processing
        this.errorManager = null;
    }

    /**
     * Log error with detailed information and add to errors array
     * @param {string} context - Where the error occurred
     * @param {Error} error - The error object
     * @param {object} additionalInfo - Any additional context information
     */
    logError(context, error, additionalInfo = {}) {
        const errorDetails = {
            context,
            message: error.message,
            stack: error.stack,
            timestamp: new Date().toISOString(),
            ...additionalInfo
        };

        // Log to console
        console.error(`[ERROR] ${context}: ${error.message}`);
        if (additionalInfo && Object.keys(additionalInfo).length > 0) {
            console.error(`[ERROR] Additional info:`, JSON.stringify(additionalInfo));
        }

        // Add to errors array
        this.errors.push(errorDetails);

        return errorDetails;
    }

    async run() {
        try {
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

            // Initialize queue from input
            this.initializeQueue();

            // Get current status
            const currentStatus = this.event.inputFields.cs_processing_queue_status;
            if (currentStatus !== '07_ASC_BATCH_A' && currentStatus !== '07_ASC_BATCH_B') {
                throw new Error(`Invalid status: ${currentStatus}`);
            }

            // Process the batch
            await this.processBatch();

            // Save errors before sending response
            if (this.errorManager) {
                await this.errorManager.saveErrors();
            }

            // Send response
            this.callback({
                outputFields: this.queue.toHubSpotProperties(currentStatus)
            });
        } catch (error) {
            const errorDetails = this.logError('Workflow action failed', error, {
                inputStatus: this.event.inputFields.cs_processing_queue_status,
                eventId: this.event.inputFields.hs_object_id
            });

            // Log to error manager
            if (this.errorManager) {
                this.errorManager.logError(error, {
                    eventId: this.event.inputFields.hs_object_id || this.event.objectId
                });
                await this.errorManager.saveErrors();
            }

            // Include detailed error information in the queue data
            this.callback({
                outputFields: {
                    cs_processing_queue_status: '99_WFL_ERROR',
                    cs_processing_queue_data: DataUtils.safeStringifyJson({
                        error: `Workflow failed: ${WORKFLOW_NAME} - ${errorDetails?.message || errorDetails || error.message}`,
                        workflow: WORKFLOW_NAME,
                        errors: this.errors,
                        timestamp: new Date().toISOString()
                    })
                }
            });
        }
    }

    initializeQueue() {
        if (!this.event.inputFields.cs_processing_queue_data) {
            throw new Error('Missing queue data');
        }

        try {
            this.queue = QueueManager.fromProperty(this.event.inputFields.cs_processing_queue_data);

            // Count the actual number of accounts to process
            let totalAccountsToProcess = 0;
            if (this.queue.data.items.remaining && this.queue.data.items.remaining.length > 0) {
                this.queue.data.items.remaining.forEach(item => {
                    const registered = Array.isArray(item.registered) ? item.registered.length : 0;
                    const attended = Array.isArray(item.attended) ? item.attended.length : 0;
                    const noShow = Array.isArray(item.noShow) ? item.noShow.length : 0;
                    totalAccountsToProcess += registered + attended + noShow;
                });
            }

            console.log('[INFO] Queue initialized with:');
            console.log(`- ${this.queue.data.items.remaining.length} event batches containing ${totalAccountsToProcess} accounts to process`);
            console.log(`- ${this.queue.data.items.processed.length} processed accounts`);
            console.log(`- ${this.queue.data.items.failed.length} failed accounts`);
        } catch (error) {
            this.logError('Failed to parse queue data', error, {
                queueData: this.event.inputFields.cs_processing_queue_data
            });
            if (this.errorManager) {
                this.errorManager.logError(error, {
                    eventId: this.event.inputFields.hs_object_id || this.event.objectId
                }, {
                    queueRelated: true
                });
            }
            throw new Error(`Failed to parse queue data: ${error.message}`);
        }
    }

    async processBatch() {
        // Check if we have any data to process
        if (!this.queue.data.items.remaining.length) {
            console.log('No items to process');
            return;
        }

        const associationData = this.queue.data.items.remaining[0];
        if (!associationData) {
            console.log('No association data found');
            return;
        }

        const { eventId, registered = [], attended = [], noShow = [] } = associationData;
        console.log(`Processing associations for event ${eventId}`);

        // Get all unique account IDs
        const allAccountIds = [...new Set([...registered, ...attended, ...noShow])];
        console.log(`Processing ${allAccountIds.length} unique accounts`);

        // Find existing contacts
        const { contactMap, notFoundIds } = await this.findContacts(allAccountIds);

        // Track results
        const successful = [];
        const failed = [];

        // Add not found contacts to failed items and log errors
        if (notFoundIds.length > 0 && this.errorManager) {
            notFoundIds.forEach(accountId => {
                this.errorManager.logError(new Error('Contact not found in HubSpot'), {
                    accountId: accountId,
                    eventId: eventId
                }, {
                    field: 'account_id',
                    integration: 'hubspot'
                });
                failed.push(accountId);
            });
        } else {
            notFoundIds.forEach(id => failed.push(id));
        }

        // Remove notFoundIds from all registration arrays to prevent duplicate processing
        const notFoundSet = new Set(notFoundIds.map(id => String(id)));
        const filterNotFound = (arr) => arr.filter(id => !notFoundSet.has(String(id)));

        const registeredFiltered = filterNotFound(registered);
        const attendedFiltered = filterNotFound(attended);
        const noShowFiltered = filterNotFound(noShow);

        // Update the original arrays
        registered.length = 0;
        registered.push(...registeredFiltered);
        attended.length = 0;
        attended.push(...attendedFiltered);
        noShow.length = 0;
        noShow.push(...noShowFiltered);

        // Process one batch type
        await this.processContactBatch(registered, 'registered', contactMap, eventId, successful, failed);

        if (registered.length === 0) {
            await this.processContactBatch(attended, 'attended', contactMap, eventId, successful, failed);
        }

        if (registered.length === 0 && attended.length === 0) {
            await this.processContactBatch(noShow, 'no_show', contactMap, eventId, successful, failed);
        }

        // If all contacts processed, remove this event
        if (registered.length === 0 && attended.length === 0 && noShow.length === 0) {
            this.queue.data.items.remaining.shift();
        }

        // Update queue
        this.queue.data.items.processed.push(...DataUtils.ensureStringIds(successful));
        this.queue.data.items.failed.push(...DataUtils.ensureStringIds(failed));
        this.queue.data.metadata.processedCount += successful.length;
        this.queue.data.metadata.failedCount += failed.length;
        this.queue.data.metadata.lastProcessedTimestamp = new Date().toISOString();

        console.log(`Processed ${successful.length} successful, ${failed.length} failed`);
    }

    async findContacts(accountIds) {
        console.log(`Finding contacts for ${accountIds.length} account IDs`);

        const contactMap = {};
        const notFoundIds = [];

        // Process in batches of 100
        for (let i = 0; i < accountIds.length; i += 100) {
            const batchIds = accountIds.slice(i, i + 100).map(id => String(id).trim());

            console.log(`[INFO] Processing batch ${i / 100 + 1}, size=${batchIds.length}`);

            let after = undefined;
            let totalFetched = 0;

            try {
                do {
                    const searchRequest = {
                        filterGroups: [{
                            filters: [{
                                propertyName: 'cs_account_id',
                                operator: 'IN',
                                values: batchIds
                            }]
                        }],
                        properties: ['cs_account_id'],
                        limit: 100,
                        after,
                        archived: false
                    };

                    const response = await this.hubspotClient.crm.contacts.searchApi.doSearch(searchRequest);

                    totalFetched += response.results.length;

                    // Track found IDs
                    const foundIds = new Set();
                    response.results.forEach(contact => {
                        const accountId = contact.properties?.cs_account_id?.trim();
                        if (accountId) {
                            // Store both string and number versions of the ID for lookup
                            contactMap[String(accountId)] = contact.id;
                            contactMap[Number(accountId)] = contact.id;
                            foundIds.add(String(accountId));
                        }
                    });

                    after = response.paging?.next?.after;
                } while (after);

                console.log(`[INFO] Batch fetched ${totalFetched} contacts`);

                // Check which weren't found (after all pages processed)
                const foundIdsSet = new Set();
                for (const key in contactMap) {
                    if (key !== 'NaN') foundIdsSet.add(String(key));
                }
                batchIds.forEach(id => {
                    if (!foundIdsSet.has(String(id).trim())) {
                        notFoundIds.push(id);
                    }
                });
            } catch (error) {
                this.logError('Error searching contacts', error, {
                    batchSize: batchIds.length,
                    batchStartIndex: i,
                    sampleIds: batchIds.slice(0, 5)
                });

                // Log to error manager
                if (this.errorManager) {
                    this.errorManager.logError(error, {
                        eventId: this.event.inputFields.hs_object_id || this.event.objectId
                    }, {
                        apiEndpoint: '/crm/v3/objects/contacts/search',
                        integration: 'hubspot'
                    });
                }

                batchIds.forEach(id => {
                    if (!contactMap[id]) {
                        notFoundIds.push(id);
                    }
                });
            }
        }

        return { contactMap, notFoundIds };
    }

    async processContactBatch(contactList, status, contactMap, eventId, successful, failed) {
        if (contactList.length === 0) return;

        // Process only a batch
        const batch = contactList.slice(0, CONFIG.BATCH_SIZE);
        console.log(`Processing ${status} batch of ${batch.length} contacts (${contactList.length - batch.length} will remain for next batch)`);

        // Remove processed items
        contactList.splice(0, CONFIG.BATCH_SIZE);

        // Prepare contacts with status
        const contactsWithStatus = [];
        let notFoundInBatch = 0;

        for (const accountId of batch) {
            // Try both string and number versions of the account ID
            const contactId = contactMap[accountId] || contactMap[String(accountId)] || contactMap[Number(accountId)];
            if (contactId) {
                contactsWithStatus.push({ contactId, status, accountId });
            } else {
                // Don't log error here - already logged in processBatch()
                failed.push(accountId);
                notFoundInBatch++;
            }
        }

        // Add summary log instead of individual logs
        if (notFoundInBatch > 0) {
            console.log(`[WARN] ${notFoundInBatch} contacts not found in HubSpot for ${status} batch`);
        }

        console.log(`Found ${contactsWithStatus.length} contacts for association (${notFoundInBatch} not found)`);

        // Process the batch
        if (contactsWithStatus.length > 0) {
            try {
                await this.createAssociations(
                    contactsWithStatus.map(({ contactId, status }) => ({ contactId, status })),
                    eventId
                );

                // Add to successful list
                contactsWithStatus.forEach(({ accountId }) => {
                    successful.push(accountId);
                });

                console.log(`[SUCCESS] Created ${contactsWithStatus.length} ${status} associations`);
            } catch (error) {
                const errorDetails = this.logError(`Failed to create ${status} associations`, error, {
                    eventId,
                    status,
                    contactCount: contactsWithStatus.length,
                    sampleContactIds: contactsWithStatus.slice(0, 3).map(c => c.contactId)
                });

                // Log to error manager
                if (this.errorManager) {
                    contactsWithStatus.forEach(({ contactId, accountId }) => {
                        this.errorManager.logError(error, {
                            contactId: contactId,
                            accountId: accountId,
                            eventId: eventId
                        }, {
                            apiEndpoint: `/crm/v4/associations/contacts/events/batch/create`,
                            integration: 'hubspot'
                        });
                    });
                }

                // Mark all as failed
                contactsWithStatus.forEach(({ accountId }) => {
                    failed.push(accountId);
                });
                console.log(`[FAIL] Marked ${contactsWithStatus.length} ${status} contacts as failed`);
            }
        }
    }

    async createAssociations(contactsWithStatus, eventId) {
        const eventObjectType = process.env.HUBSPOT_EVENT_OBJECT_ID;

        if (!eventObjectType) {
            throw new Error('HUBSPOT_EVENT_OBJECT_ID not set');
        }

        if (!contactsWithStatus || contactsWithStatus.length === 0) {
            throw new Error('No contacts to associate');
        }

        // Create inputs array
        const inputs = contactsWithStatus.map(({ contactId, status }) => {
            // Determine association types
            let associationTypes = [];

            switch (status) {
                case 'attended':
                    associationTypes = [
                        {
                            "associationCategory": "USER_DEFINED",
                            "associationTypeId": ASSOCIATION_TYPES.REGISTERED
                        },
                        {
                            "associationCategory": "USER_DEFINED",
                            "associationTypeId": ASSOCIATION_TYPES.ATTENDEE
                        }
                    ];
                    break;
                case 'no_show':
                    associationTypes = [
                        {
                            "associationCategory": "USER_DEFINED",
                            "associationTypeId": ASSOCIATION_TYPES.REGISTERED
                        },
                        {
                            "associationCategory": "USER_DEFINED",
                            "associationTypeId": ASSOCIATION_TYPES.NO_SHOW
                        }
                    ];
                    break;
                default:
                    associationTypes = [
                        {
                            "associationCategory": "USER_DEFINED",
                            "associationTypeId": ASSOCIATION_TYPES.REGISTERED
                        }
                    ];
                    break;
            }

            return {
                "from": {
                    "id": contactId,
                    "type": "contacts"
                },
                "to": {
                    "id": eventId,
                    "type": eventObjectType
                },
                "types": associationTypes
            };
        });

        console.log(`Creating ${inputs.length} associations with event:${eventId}`);

        try {
            // Make API call
            const response = await this.hubspotClient.apiRequest({
                method: 'POST',
                path: `/crm/v4/associations/contacts/${eventObjectType}/batch/create`,
                body: { inputs },
                headers: {
                    'Content-Type': 'application/json'
                }
            });

            console.log(`Association response status: ${response.status}`);

            // Check for error status codes
            if (response.status >= 400) {
                const errorDetails = {
                    status: response.status,
                    statusText: response.statusText,
                    responseBody: response.body
                };

                this.logError('HubSpot API error', new Error(`API returned status ${response.status}`), errorDetails);
                if (this.errorManager) {
                    this.errorManager.logError(new Error(`API returned status ${response.status}`), {
                        eventId: eventId
                    }, {
                        apiEndpoint: `/crm/v4/associations/contacts/${eventObjectType}/batch/create`,
                        integration: 'hubspot'
                    });
                }
                throw new Error(`HubSpot API error: ${response.status} ${response.statusText}`);
            }

            return true;
        } catch (error) {
            // Capture API errors with more detail
            if (error.response) {
                const errorDetails = {
                    status: error.response.status,
                    statusText: error.response.statusText,
                    responseBody: error.response.body || error.response.data,
                    eventId,
                    eventObjectType,
                    contactCount: inputs.length
                };

                this.logError('HubSpot API error', error, errorDetails);
                if (this.errorManager) {
                    this.errorManager.logError(error, {
                        eventId: eventId
                    }, {
                        apiEndpoint: `/crm/v4/associations/contacts/${eventObjectType}/batch/create`,
                        integration: 'hubspot'
                    });
                }
                throw new Error(`HubSpot API error: ${error.message}`);
            } else {
                // Network or other errors
                this.logError('Association creation error', error, {
                    eventId,
                    eventObjectType,
                    contactCount: inputs.length
                });
                if (this.errorManager) {
                    this.errorManager.logError(error, {
                        eventId: eventId
                    }, {
                        apiEndpoint: `/crm/v4/associations/contacts/${eventObjectType}/batch/create`,
                        integration: 'hubspot'
                    });
                }
                throw error;
            }
        }
    }
}

// Export the main function
module.exports.main = async (event, callback) => {
    console.log('Beginning association batch processing');
    const processor = new AssociationProcessor(event, callback);
    await processor.run();
};