/**
 * HubSpot Workflow Action: Fetch Event Updates and Registrations
 * -----------------------------------------------------------
 * For Workflow: [Events] | 02 Fetch Event Updates and Registrations
 * 
 * Description:
 * Updates event details and then fetches registrations for an event from ConnectSpace.
 * Triggered when cs_processing_schedule_time occurs.
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
 * 
 * Output Properties:
 * - cs_processing_queue_status: String (Next workflow step or error)
 * - cs_processing_queue_data: String (Queue data for next step)
 */

// Required HubSpot packages
const hubspot = require('@hubspot/api-client');
const axios = require('axios');

// Configuration Constants
const CONFIG = {
    BATCH_SIZE: 50,  // Number of events to process in each batch
    RETRY_LIMIT: 3   // Maximum number of retries for failed operations
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
                ...metadata
            },
            items: {
                remaining: [],
                processed: [],
                failed: []
            }
        };

        // Ensure all IDs in arrays are strings for consistency
        this.ensureConsistentDataTypes();
    }

    ensureConsistentDataTypes() {
        // Ensure all metadata values that should be strings are strings
        if (this.data.metadata) {
            if (this.data.metadata.eventId) {
                this.data.metadata.eventId = DataUtils.ensureString(this.data.metadata.eventId);
            }
            if (this.data.metadata.connectSpaceEventId) {
                this.data.metadata.connectSpaceEventId = DataUtils.ensureString(this.data.metadata.connectSpaceEventId);
            }
        }

        // Ensure all IDs in arrays are strings
        if (this.data.items) {
            if (Array.isArray(this.data.items.processed)) {
                this.data.items.processed = DataUtils.ensureStringIds(this.data.items.processed);
            }

            if (Array.isArray(this.data.items.failed)) {
                this.data.items.failed = DataUtils.ensureStringIds(this.data.items.failed);
            }

            if (Array.isArray(this.data.items.remaining)) {
                this.data.items.remaining = DataUtils.ensureStringIds(this.data.items.remaining);
            }
        }
    }

    setItems(items) {
        this.data.items.remaining = DataUtils.ensureStringIds(items);
        this.data.metadata.totalCount = items.length;
        return this;
    }

    getNextBatch() {
        return this.data.items.remaining.slice(0, CONFIG.BATCH_SIZE);
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
            .slice(CONFIG.BATCH_SIZE);
        this.data.metadata.currentBatch++;
        return this;
    }

    getWorkflowStatus() {
        // For EVENT_BATCH (initial fetch), failures are critical
        if (this.data.processType === 'EVENT_BATCH' && this.data.metadata.failedCount > 0) {
            return '99_WFL_ERROR';
        }

        // For other process types, continue despite failures
        // Failures are tracked in the queue data for later review
        return '03_REG_BATCH_A';
    }

    static getWorkflowStatuses() {
        return [
            '01_EVT_BATCH_A',
            '01_EVT_BATCH_B',
            '01C_EVT_SCHEDULE',
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

    toHubSpotProperties() {
        return {
            cs_processing_queue_status: this.getWorkflowStatus(),
            cs_processing_queue_data: DataUtils.safeStringifyJson(this.data)
        };
    }

    static fromProperty(property) {
        try {
            // Handle both JSON string and object
            let data;
            if (typeof property === 'string') {
                data = DataUtils.safeParseJson(property);
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

            // Ensure all IDs are strings for consistency
            queue.ensureConsistentDataTypes();

            return queue;
        } catch (error) {
            console.error('Failed to parse queue data:', error);
            return null;
        }
    }
}

// Base Workflow Action Class
class WorkflowAction {
    constructor(event, callback) {
        this.event = event;
        this.callback = callback;
        this.hubspotClient = null;
        this.connectSpaceClient = null;
    }

    async initialize() {
        EnvironmentValidator.validate([
            'HUBSPOT_ACCESS_TOKEN',
            'CONNECT_SPACE_API_KEY',
            'CONNECT_SPACE_BASE_URL',
            'HUBSPOT_EVENT_OBJECT_ID'
        ]);

        this.hubspotClient = APIClientFactory.createHubSpotClient(
            process.env.HUBSPOT_ACCESS_TOKEN
        );

        this.connectSpaceClient = APIClientFactory.createConnectSpaceClient(
            process.env.CONNECT_SPACE_API_KEY,
            process.env.CONNECT_SPACE_BASE_URL
        );
    }

    async execute() {
        throw new Error('Execute method must be implemented by child class');
    }

    async run() {
        try {
            await this.initialize();
            await this.execute();
        } catch (error) {
            console.error('Workflow action failed:', error);
            this.callback({
                outputFields: {
                    cs_processing_queue_status: '99_WFL_ERROR',
                    cs_processing_queue_data: DataUtils.safeStringifyJson({
                        error: error.message,
                        timestamp: new Date().toISOString()
                    })
                }
            });
        }
    }
}

// Event Action Base Class
class EventActionBase extends WorkflowAction {
    constructor(event, callback) {
        super(event, callback);
        this.queue = null;
    }

    initializeQueue(processType, metadata = {}) {
        this.queue = new QueueManager(processType, metadata);
        return this.queue;
    }

    loadQueue() {
        if (this.event.inputFields.cs_processing_queue_data) {
            this.queue = QueueManager.fromProperty(
                this.event.inputFields.cs_processing_queue_data
            );
            if (this.queue && this.queue.data && this.queue.data.metadata) {
                console.log('Queue loaded - Process:', this.queue.data.processType,
                    '| Total:', this.queue.data.metadata.totalCount,
                    '| Failed:', this.queue.data.metadata.failedCount);
            } else if (this.queue) {
                console.warn('Queue loaded but metadata is missing');
            } else {
                console.warn('Failed to parse queue data');
            }
        }
        return this.queue;
    }

    async sendQueueResponse() {
        if (!this.queue) {
            throw new Error('Queue not initialized');
        }

        const outputFields = this.queue.toHubSpotProperties();

        // Optionally update HubSpot directly via API
        if (process.env.UPDATE_HUBSPOT_DIRECTLY === 'true') {
            const eventId = this.getEventId();
            console.log('\n=== Updating HubSpot Event via API ===');
            await this.updateHubSpotEventProperties(eventId, outputFields);
        }

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

    getEventId() {
        return DataUtils.ensureString(this.event.inputFields.hs_object_id || this.event.objectId);
    }

    getConnectSpaceEventId() {
        return DataUtils.ensureString(this.event.inputFields.cs_event_id);
    }
}

// Action Implementation
class FetchRegistrationsAction extends EventActionBase {
    async execute() {
        const eventId = this.getEventId();
        console.log('\nProcessing Event:', eventId);

        // Try to load existing queue data first
        this.loadQueue();

        // If queue exists AND is the correct process type, we're continuing from a previous step
        // This workflow action only handles EVENT_BATCH process type
        if (this.queue && this.queue.data.processType === 'EVENT_BATCH') {
            console.log('Resuming workflow - returning queue\n');
            return this.sendQueueResponse();
        }

        // If queue exists but is wrong type, log warning and create fresh queue
        if (this.queue && this.queue.data.processType !== 'EVENT_BATCH') {
            console.warn(`⚠️  Warning: Queue has wrong process type: ${this.queue.data.processType}`);
            console.warn('    Expected: EVENT_BATCH - Creating fresh queue instead\n');
            this.queue = null; // Clear the invalid queue
        }

        // No queue data - this is the first run
        console.log('First run - initializing queue');

        // Get the ConnectSpace event ID (should be in input fields)
        const connectSpaceEventId = await this.getConnectSpaceEventIdFromInput(eventId);
        if (!connectSpaceEventId) {
            throw new Error('ConnectSpace event ID not found');
        }

        console.log('ConnectSpace event ID:', connectSpaceEventId);

        // Initialize queue with registrations
        this.initializeQueue('EVENT_BATCH', {
            eventId: DataUtils.ensureString(eventId),
            connectSpaceEventId: DataUtils.ensureString(connectSpaceEventId)
        });

        // Update event with latest data from ConnectSpace
        await this.updateEventWithLatestData(eventId, connectSpaceEventId);
        console.log('Event properties updated');

        // Fetch registrations
        const registrations = await this.fetchAllRegistrations(connectSpaceEventId);
        if (registrations.length === 0) {
            console.log('No registrations found\n');
            return this.sendQueueResponse();
        }

        console.log(`Found ${registrations.length} registrations`);

        // Only extract the registration IDs for processing
        const registrationIds = registrations.map(reg => DataUtils.ensureString(reg.id));

        // Update queue with registration IDs
        this.queue.setItems(registrationIds);

        // Send response
        await this.sendQueueResponse();
    }

    async getConnectSpaceEventIdFromInput(eventId) {
        // Use the cs_event_id from input fields (sent directly from HubSpot)
        if (this.event.inputFields.cs_event_id) {
            return DataUtils.ensureString(this.event.inputFields.cs_event_id);
        }

        // Fallback: If not in input fields, fetch it from HubSpot
        // This should rarely happen if the workflow is configured correctly
        console.warn('cs_event_id not in input fields, fetching from HubSpot API');
        try {
            const response = await this.hubspotClient.crm.objects.basicApi.getById(
                process.env.HUBSPOT_EVENT_OBJECT_ID,
                eventId,
                ['cs_event_id']
            );

            return DataUtils.ensureString(response.properties.cs_event_id);
        } catch (error) {
            console.error('Error fetching ConnectSpace event ID:', error);
            throw new Error(`Failed to get ConnectSpace event ID: ${error.message}`);
        }
    }

    async updateEventWithLatestData(eventId, connectSpaceEventId) {
        try {
            // Fetch event details from ConnectSpace
            const response = await this.connectSpaceClient.get(`/events/${connectSpaceEventId}`);
            const connectSpaceEvent = response.data.event || response.data;

            if (!connectSpaceEvent) {
                console.log('No event found in ConnectSpace');
                return;
            }

            try {
                // Get the schema to see what properties actually exist
                const schemaResponse = await this.hubspotClient.crm.schemas.coreApi.getById(
                    process.env.HUBSPOT_EVENT_OBJECT_ID
                );
                console.log('Retrieved schema for event object');

                // Create a clean properties object with only valid, writable properties
                const updatedProperties = {};

                // Store the ConnectSpace event ID (this should always exist)
                updatedProperties.cs_event_id = DataUtils.ensureString(connectSpaceEvent.id);

                // Map other properties only if they exist in the schema
                const propertyNames = schemaResponse.properties.map(p => p.name);

                // Define HubSpot property types
                const PROPERTY_TYPES = {
                    DATE: 'date',
                    DATETIME: 'datetime',
                    NUMBER: 'number',
                    BOOLEAN: 'boolean',
                    ENUMERATION: 'enumeration',
                    STRING: 'string'
                };

                // Helper function to format values based on type
                const formatHubSpotValue = (value, type) => {
                    if (value === null || value === undefined) return value;

                    switch (type) {
                        case PROPERTY_TYPES.DATE:
                            return new Date(value).getTime();
                        case PROPERTY_TYPES.DATETIME:
                            return new Date(value).getTime();
                        case PROPERTY_TYPES.NUMBER:
                            return Number(value);
                        case PROPERTY_TYPES.BOOLEAN:
                            return Boolean(value);
                        case PROPERTY_TYPES.ENUMERATION:
                            return String(value);
                        default:
                            return String(value);
                    }
                };

                // Define property mappings with type information
                const propertyMappings = [
                    // Core Event Properties
                    { hubspotProp: 'cs_event_id', connectSpaceProp: 'id', type: PROPERTY_TYPES.STRING },
                    { hubspotProp: 'slug', connectSpaceProp: 'slug', type: PROPERTY_TYPES.STRING },
                    { hubspotProp: 'name', connectSpaceProp: 'name', type: PROPERTY_TYPES.STRING },
                    { hubspotProp: 'description', connectSpaceProp: 'description', type: PROPERTY_TYPES.STRING },
                    { hubspotProp: 'event_header', connectSpaceProp: 'event_header', type: PROPERTY_TYPES.STRING },

                    // Date and Time Properties
                    { hubspotProp: 'start_date', connectSpaceProp: 'start_date', type: PROPERTY_TYPES.DATE },
                    { hubspotProp: 'start_time', connectSpaceProp: 'start_time', type: PROPERTY_TYPES.STRING },
                    { hubspotProp: 'show_start_time', connectSpaceProp: 'show_start_time', type: PROPERTY_TYPES.BOOLEAN },
                    { hubspotProp: 'starts_at', connectSpaceProp: 'starts_at', type: PROPERTY_TYPES.DATETIME },
                    { hubspotProp: 'end_date', connectSpaceProp: 'end_date', type: PROPERTY_TYPES.DATE },
                    { hubspotProp: 'end_time', connectSpaceProp: 'end_time', type: PROPERTY_TYPES.STRING },
                    { hubspotProp: 'show_end_time', connectSpaceProp: 'show_end_time', type: PROPERTY_TYPES.BOOLEAN },
                    { hubspotProp: 'ends_at', connectSpaceProp: 'ends_at', type: PROPERTY_TYPES.DATETIME },
                    { hubspotProp: 'archive_date', connectSpaceProp: 'archive_date', type: PROPERTY_TYPES.DATE },

                    // Registration and Publishing Properties
                    { hubspotProp: 'registration_instructions', connectSpaceProp: 'registration_instructions', type: PROPERTY_TYPES.STRING },
                    { hubspotProp: 'allow_registrations', connectSpaceProp: 'allow_registrations', type: PROPERTY_TYPES.BOOLEAN },
                    { hubspotProp: 'published_at', connectSpaceProp: 'published_at', type: PROPERTY_TYPES.DATETIME },
                    { hubspotProp: 'active_registration_count', connectSpaceProp: 'active_registration_count', type: PROPERTY_TYPES.NUMBER },
                    { hubspotProp: 'hide_agenda', connectSpaceProp: 'hide_agenda', type: PROPERTY_TYPES.BOOLEAN },

                    // Web Properties
                    { hubspotProp: 'website', connectSpaceProp: 'website', type: PROPERTY_TYPES.STRING },
                    { hubspotProp: 'image_url', connectSpaceProp: 'image_url', type: PROPERTY_TYPES.STRING },
                    { hubspotProp: 'background_image_url', connectSpaceProp: 'background_image_url', type: PROPERTY_TYPES.STRING },
                    { hubspotProp: 'publish_on_events_page', connectSpaceProp: 'publish_on_events_page', type: PROPERTY_TYPES.BOOLEAN },
                    { hubspotProp: 'redirect_url', connectSpaceProp: 'redirect_url', type: PROPERTY_TYPES.STRING },

                    // Time Zone Properties
                    { hubspotProp: 'time_format', connectSpaceProp: 'time_format', type: PROPERTY_TYPES.STRING },
                    { hubspotProp: 'time_zone', connectSpaceProp: 'time_zone', type: PROPERTY_TYPES.STRING },
                    { hubspotProp: 'time_zone_offset', connectSpaceProp: 'time_zone_offset', type: PROPERTY_TYPES.STRING },

                    // Status Properties
                    { hubspotProp: 'published', connectSpaceProp: 'published', type: PROPERTY_TYPES.BOOLEAN },
                    { hubspotProp: 'archived', connectSpaceProp: 'archived', type: PROPERTY_TYPES.BOOLEAN },

                    // Timestamps
                    { hubspotProp: 'created_at', connectSpaceProp: 'created_at', type: PROPERTY_TYPES.DATETIME },
                    { hubspotProp: 'updated_at', connectSpaceProp: 'updated_at', type: PROPERTY_TYPES.DATETIME },

                    // Location Properties
                    { hubspotProp: 'location_id', connectSpaceProp: 'location_id', type: PROPERTY_TYPES.STRING },
                    { hubspotProp: 'location_name', connectSpaceProp: 'location.name', type: PROPERTY_TYPES.STRING },
                    { hubspotProp: 'address_1', connectSpaceProp: 'location.address_1', type: PROPERTY_TYPES.STRING },
                    { hubspotProp: 'address_2', connectSpaceProp: 'location.address_2', type: PROPERTY_TYPES.STRING },
                    { hubspotProp: 'city', connectSpaceProp: 'location.city', type: PROPERTY_TYPES.STRING },
                    { hubspotProp: 'state', connectSpaceProp: 'location.state', type: PROPERTY_TYPES.STRING },
                    { hubspotProp: 'zip', connectSpaceProp: 'location.zip', type: PROPERTY_TYPES.STRING },
                    { hubspotProp: 'country', connectSpaceProp: 'location.country', type: PROPERTY_TYPES.STRING }
                ];

                // Update properties based on mappings
                for (const mapping of propertyMappings) {
                    if (propertyNames.indexOf(mapping.hubspotProp) !== -1) {
                        const connectSpacePropPath = mapping.connectSpaceProp.split('.');
                        let value = connectSpaceEvent;
                        let validPath = true;

                        for (const pathPart of connectSpacePropPath) {
                            if (value && value[pathPart] !== undefined) {
                                value = value[pathPart];
                            } else {
                                validPath = false;
                                break;
                            }
                        }

                        if (validPath && value !== null && value !== undefined) {
                            updatedProperties[mapping.hubspotProp] = formatHubSpotValue(value, mapping.type);
                        }
                    }
                }

                // Save updated event to HubSpot
                await this.hubspotClient.crm.objects.basicApi.update(
                    process.env.HUBSPOT_EVENT_OBJECT_ID,
                    eventId,
                    { properties: updatedProperties }
                );
                console.log('Event updated with latest data from ConnectSpace');
            } catch (schemaError) {
                console.error('Failed to retrieve schema:', schemaError);
                // Fallback approach - just update the cs_event_id
                await this.hubspotClient.crm.objects.basicApi.update(
                    process.env.HUBSPOT_EVENT_OBJECT_ID,
                    eventId,
                    {
                        properties: {
                            cs_event_id: DataUtils.ensureString(connectSpaceEvent.id)
                        }
                    }
                );
                console.log('Updated cs_event_id only');
            }
        } catch (error) {
            console.error('Failed to update event with latest data:', error);
            // Continue execution even if update fails
        }
    }

    async fetchAllRegistrations(connectSpaceEventId) {
        try {
            let page = 1;
            let allRegistrations = [];
            let hasMore = true;

            while (hasMore) {
                const response = await this.connectSpaceClient.get(
                    `/events/${connectSpaceEventId}/registrations`,
                    { params: { per_page: 100, page } }
                );

                // Handle different response formats
                let registrations = [];
                if (response.data && Array.isArray(response.data)) {
                    registrations = response.data;
                } else if (response.data && response.data.registrations && Array.isArray(response.data.registrations)) {
                    registrations = response.data.registrations;
                } else if (response.data && response.data.data && Array.isArray(response.data.data)) {
                    registrations = response.data.data;
                } else {
                    console.warn('Unexpected response format:', JSON.stringify(response.data).substring(0, 200));
                    registrations = [];
                }

                allRegistrations = allRegistrations.concat(registrations);
                hasMore = registrations.length >= 100;
                page++;
            }

            // Deduplicate by account_id if present
            if (allRegistrations.length > 0 && allRegistrations[0].account_id) {
                const uniqueRegistrations = new Map();
                allRegistrations.forEach(reg => {
                    if (!reg.account_id) return;
                    const accountIdStr = DataUtils.ensureString(reg.account_id);
                    const previous = uniqueRegistrations.get(accountIdStr);
                    if (!previous || new Date(reg.created_at) > new Date(previous.created_at)) {
                        uniqueRegistrations.set(accountIdStr, reg);
                    }
                });
                return Array.from(uniqueRegistrations.values());
            }

            return allRegistrations;
        } catch (error) {
            console.error('Error fetching registrations from ConnectSpace:', error);
            // Return empty array instead of throwing to prevent workflow failure
            return [];
        }
    }
}

exports.main = async (event, callback) => {
    const action = new FetchRegistrationsAction(event, callback);
    await action.run();
};
